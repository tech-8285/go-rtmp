//
// Copyright (c) 2018- yutopp (yutopp@gmail.com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at  https://www.boost.org/LICENSE_1_0.txt)
//

package rtmp

import (
	"context"
	"io"
	"math"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/yutopp/go-rtmp/message"
)

const ctrlMsgChunkStreamID = 2

const maxWriterQueueSize = 64

type ChunkMessage struct {
	StreamID uint32
	Message  message.Message
}

// scheduledWriter pairs a writer with the message epoch at which it was
// queued. Stale entries left over after an abort carry an older epoch and are
// discarded by the scheduler without touching the wire.
type scheduledWriter struct {
	writer *ChunkStreamWriter
	epoch  uint64
}

// abortRequest asks the scheduler goroutine to abort the in-flight message of
// a single chunk stream. The reply channel is closed once the AbortMessage has
// been sent and the writer has been reset.
type abortRequest struct {
	chunkStreamID int
	replyCh       chan struct{}
}

type ChunkStreamer struct {
	r *ChunkStreamerReader
	w *ChunkStreamerWriter

	readers map[int]*ChunkStreamReader
	writers map[int]*ChunkStreamWriter
	mu      sync.Mutex

	writerSched *chunkStreamerWriterSched

	msgDec *message.Decoder
	msgEnc *message.Encoder

	selfState *StreamControlState
	peerState *StreamControlState

	err  error
	done chan struct{}

	controlStreamWriter func(chunkStreamID int, timestamp uint32, msg message.Message) error

	cacheBuffer []byte
	config      *StreamControlStateConfig
	logger      logrus.FieldLogger
}

func NewChunkStreamer(r io.Reader, w io.Writer, config *StreamControlStateConfig) *ChunkStreamer {
	if config == nil {
		config = defaultStreamControlStateConfig
	}

	cs := &ChunkStreamer{
		r: &ChunkStreamerReader{
			reader: r,
		},
		w: &ChunkStreamerWriter{
			writer: w,
		},

		readers: make(map[int]*ChunkStreamReader),
		writers: make(map[int]*ChunkStreamWriter),

		writerSched: &chunkStreamerWriterSched{
			writers: make(chan scheduledWriter, maxWriterQueueSize),
			abortCh: make(chan abortRequest, maxWriterQueueSize),
			stopCh:  make(chan struct{}),
		},

		msgDec: message.NewDecoder(nil),
		msgEnc: message.NewEncoder(nil),

		selfState: NewStreamControlState(config),
		peerState: NewStreamControlState(config),

		done: make(chan struct{}),

		cacheBuffer: make([]byte, 64*1024), // cache 64KB
		config:      config,
		logger:      logrus.StandardLogger(),
	}
	cs.writerSched.streamer = cs
	go cs.schedWriteLoop()

	return cs
}

func (cs *ChunkStreamer) Read(cmsg *ChunkMessage) (int, uint32, error) {
	reader, err := cs.NewChunkReader()
	if err != nil {
		return 0, 0, err
	}

	cs.msgDec.Reset(reader)
	if err := cs.msgDec.Decode(message.TypeID(reader.messageTypeID), &cmsg.Message); err != nil {
		return 0, 0, err
	}

	cmsg.StreamID = reader.messageStreamID

	return reader.basicHeader.chunkStreamID, uint32(reader.timestamp), nil
}

func (cs *ChunkStreamer) Write(
	ctx context.Context, // NOTE: Retire writing when a current chunk is busy
	chunkStreamID int,
	timestamp uint32,
	cmsg *ChunkMessage,
) error {
	writer, err := cs.NewChunkWriter(ctx, chunkStreamID)
	if err != nil {
		return err
	}
	//defer writer.Close()

	cs.msgEnc.Reset(writer)
	if err := cs.msgEnc.Encode(cmsg.Message); err != nil {
		return err
	}
	writer.timestamp = timestamp
	writer.messageLength = uint32(writer.buf.Len())
	writer.messageTypeID = byte(cmsg.Message.TypeID())
	writer.messageStreamID = cmsg.StreamID

	return cs.Sched(writer)
}

// WriteWithDeadline schedules a message exactly like Write, but it also waits
// for the whole message to be flushed. If the context is canceled (for example
// because a send deadline expired) before the message completed, the remaining
// chunks are aborted: an AbortMessage is delivered to the peer, the unfinished
// message is dropped and ErrMessageAborted is returned. The same chunk stream is
// immediately reusable for another message afterwards.
//
// Passing context.Background() keeps the ordinary, non-deadlined behavior: the
// call blocks until the message has been fully written.
func (cs *ChunkStreamer) WriteWithDeadline(
	ctx context.Context,
	chunkStreamID int,
	timestamp uint32,
	cmsg *ChunkMessage,
) error {
	writer, err := cs.NewChunkWriter(ctx, chunkStreamID)
	if err != nil {
		return err
	}

	cs.msgEnc.Reset(writer)
	if err := cs.msgEnc.Encode(cmsg.Message); err != nil {
		return err
	}
	writer.timestamp = timestamp
	writer.messageLength = uint32(writer.buf.Len())
	writer.messageTypeID = byte(cmsg.Message.TypeID())
	writer.messageStreamID = cmsg.StreamID

	if err := cs.Sched(writer); err != nil {
		return err
	}

	// Wait for completion. A canceled context means the send deadline expired
	// before the message finished, therefore abort the rest of its chunks.
	if err := writer.Wait(ctx); err != nil {
		if ctx.Err() != nil {
			if abortErr := cs.Abort(chunkStreamID); abortErr != nil {
				return errors.Wrapf(abortErr, "Failed to abort message: Original error = %+v", err)
			}
			return ErrMessageAborted
		}
		return err
	}

	return nil
}

// Abort stops sending the message which is currently being written on the given
// chunk stream, sends an AbortMessage (message type id 2) to the peer so that it
// discards the partially received message, and resets the writer so that a new
// message can be sent on the same chunk stream.
// It is a no-op when no message is in flight on that chunk stream.
func (cs *ChunkStreamer) Abort(chunkStreamID int) error {
	req := abortRequest{
		chunkStreamID: chunkStreamID,
		replyCh:       make(chan struct{}),
	}

	select {
	case cs.writerSched.abortCh <- req:
	case <-cs.writerSched.stopCh:
		return errors.New("Failed to abort message: chunk streamer is closed")
	}

	select {
	case <-req.replyCh:
		return nil
	case <-cs.writerSched.stopCh:
		return errors.New("Failed to abort message: chunk streamer is closed")
	}
}

func (cs *ChunkStreamer) NewChunkReader() (*ChunkStreamReader, error) {
again:
	reader, err := cs.readChunk()
	if err != nil {
		return nil, err
	}
	if cs.r.FragmentReadBytes() >= uint32(cs.peerState.ackWindowSize/2) { // TODO: fix size
		if err := cs.sendAck(cs.r.TotalReadBytes()); err != nil {
			return nil, err
		}
		cs.r.ResetFragmentReadBytes()
	}

	if !reader.completed {
		goto again
	}

	// A completed AbortMessage asks us to drop the unfinished message of the
	// named chunk stream. It must never be delivered to upper layers.
	if reader.messageTypeID == byte(message.TypeIDAbortMessage) {
		var decoded message.Message
		cs.msgDec.Reset(reader)
		if err := cs.msgDec.Decode(message.TypeIDAbortMessage, &decoded); err != nil {
			return nil, errors.Wrapf(err, "Failed to decode AbortMessage")
		}

		cs.discardChunkReader(int(decoded.(*message.AbortMessage).ChunkStreamID))
		goto again
	}

	return reader, nil
}

// discardChunkReader drops every buffered (and possibly partial) message state
// kept for the given chunk stream, so that the next message arriving on it starts
// cleanly. Other chunk streams are not affected.
func (cs *ChunkStreamer) discardChunkReader(chunkStreamID int) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	reader, ok := cs.readers[chunkStreamID]
	if !ok {
		return
	}
	reader.buf.Reset()
	reader.completed = false
	reader.messageLength = 0
	reader.timestamp = 0
	reader.timestampDelta = 0
}

// NewChunkWriter Returns a writer for a chunkStreamID.
// Wait until writing have been finished if the writer is running.
// An aborted previous message is consumed transparently inside Wait so the same
// chunk stream can immediately be reused for a new message.
func (cs *ChunkStreamer) NewChunkWriter(ctx context.Context, chunkStreamID int) (*ChunkStreamWriter, error) {
	writer, err := cs.prepareChunkWriter(chunkStreamID)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to prepare chunk writer")
	}
	if err := writer.Wait(ctx); err != nil {
		return nil, errors.Wrapf(err, "Failed to wait chunk writer")
	}

	return writer, nil
}

// Sched marks writer as carrying a new message and enqueues it. All epoch and
// in-flight state is updated under stateM so it is synchronized with the
// scheduler goroutine (which reads the snapshot carried by the queue entry).
func (cs *ChunkStreamer) Sched(writer *ChunkStreamWriter) error {
	writer.stateM.Lock()
	writer.newChunk = true
	writer.inFlight = true
	writer.epoch++
	entry := scheduledWriter{
		writer: writer,
		epoch:  writer.epoch,
	}
	writer.stateM.Unlock()

	return cs.writerSched.Sched(entry)
}

func (cs *ChunkStreamer) SelfState() *StreamControlState {
	return cs.selfState
}

func (cs *ChunkStreamer) PeerState() *StreamControlState {
	return cs.peerState
}

func (cs *ChunkStreamer) Done() <-chan struct{} {
	return cs.done
}

func (cs *ChunkStreamer) Err() error {
	return cs.err
}

func (cs *ChunkStreamer) Close() error {
	return cs.writerSched.Close()
}

// returns nil reader when chunk is fragmented.
func (cs *ChunkStreamer) readChunk() (*ChunkStreamReader, error) {
	var bh chunkBasicHeader
	if err := decodeChunkBasicHeader(cs.r, cs.cacheBuffer, &bh); err != nil {
		return nil, err
	}
	//cs.logger.Debugf("(READ) BasicHeader = %+v", bh)

	var mh chunkMessageHeader
	if err := decodeChunkMessageHeader(cs.r, bh.fmt, cs.cacheBuffer, &mh); err != nil {
		return nil, err
	}
	//cs.logger.Debugf("(READ) MessageHeader = %+v", mh)

	reader, err := cs.prepareChunkReader(bh.chunkStreamID)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to prepare chunk reader")
	}
	if reader.completed {
		reader.buf.Reset()
		reader.completed = false
	}

	reader.basicHeader = bh
	reader.messageHeader = mh

	switch bh.fmt {
	case 0:
		reader.timestamp = mh.timestamp
		reader.timestampDelta = 0 // reset
		reader.messageLength = mh.messageLength
		reader.messageTypeID = mh.messageTypeID
		reader.messageStreamID = mh.messageStreamID

	case 1:
		reader.timestampDelta = mh.timestampDelta
		reader.messageLength = mh.messageLength
		reader.messageTypeID = mh.messageTypeID

	case 2:
		reader.timestampDelta = mh.timestampDelta

	case 3:
		// DO NOTHING

	default:
		panic("unsupported chunk") // TODO: fix
	}

	//cs.logger.Debugf("(READ) MessageLength = %d, Current = %d", reader.messageLength, reader.buf.Len())

	expectLen := int(reader.messageLength) - reader.buf.Len()
	if expectLen <= 0 {
		// A message length that does not advance the buffer means the chunk
		// stream is in an inconsistent state (for example a truncated message
		// left over before an abort was processed). Report it as an error
		// instead of panicking so the connection can fail cleanly.
		return nil, errors.Errorf(
			"invalid chunk stream state: message length = %d, buffered = %d, chunk stream id = %d",
			reader.messageLength, reader.buf.Len(), bh.chunkStreamID,
		)
	}

	if uint32(expectLen) > cs.peerState.chunkSize {
		expectLen = int(cs.peerState.chunkSize)
	}
	//cs.logger.Debugf("(READ) Length = %d", expectLen)

	lr := io.LimitReader(cs.r, int64(expectLen))
	if _, err := io.CopyBuffer(&reader.buf, lr, cs.cacheBuffer); err != nil {
		return nil, err
	}
	//cs.logger.Debugf("(READ) Buffer: %+v", reader.buf.Bytes())

	if int(reader.messageLength)-reader.buf.Len() != 0 {
		// fragmented
		return reader, nil
	}

	// read completed, update timestamp
	reader.timestamp += reader.timestampDelta
	reader.completed = true

	return reader, nil
}

func (cs *ChunkStreamer) writeChunk(writer *ChunkStreamWriter) (bool, error) {
	cs.updateWriterHeader(writer)

	//cs.logger.Debugf("(WRITE) Headers: Basic = %+v / Message = %+v", writer.basicHeader, writer.messageHeader)
	//cs.logger.Debugf("(WRITE) Buffer: %+v", writer.buf.Bytes())

	expectLen := writer.buf.Len()
	if uint32(expectLen) > cs.selfState.chunkSize {
		expectLen = int(cs.selfState.chunkSize)
	}

	if err := encodeChunkBasicHeader(cs.w, &writer.basicHeader); err != nil {
		return false, err
	}
	if err := encodeChunkMessageHeader(cs.w, writer.basicHeader.fmt, &writer.messageHeader); err != nil {
		return false, err
	}

	if _, err := io.CopyN(cs.w, writer, int64(expectLen)); err != nil {
		return false, err
	}
	if err := cs.w.Flush(); err != nil {
		return false, err
	}

	if writer.buf.Len() != 0 {
		// fragmented
		return false, nil
	}

	return true, nil
}

func (cs *ChunkStreamer) updateWriterHeader(writer *ChunkStreamWriter) {
	fmt := byte(2) // default: only timestamp delta
	if writer.messageHeader.messageLength != writer.messageLength ||
		writer.messageTypeID != writer.messageHeader.messageTypeID {
		// header or type id is updated, change fmt to 1 to notify difference and update state
		writer.messageHeader.messageLength = writer.messageLength
		writer.messageHeader.messageTypeID = writer.messageTypeID
		fmt = 1
	}
	if writer.timestamp != writer.messageHeader.timestamp || writer.newChunk {
		if writer.timestamp >= writer.messageHeader.timestamp {
			writer.timestampDelta = writer.timestamp - writer.messageHeader.timestamp
		} else {
			// timestamp is reversed, clear timestamp data
			fmt = 0
			writer.timestampDelta = 0
		}
	}
	writer.newChunk = false
	if writer.timestampDelta == writer.messageHeader.timestampDelta && fmt == 2 {
		fmt = 3
	}
	writer.messageHeader.timestampDelta = writer.timestampDelta
	writer.messageHeader.timestamp = writer.timestamp

	if writer.messageHeader.messageStreamID != writer.messageStreamID {
		fmt = 0
		writer.messageHeader.messageStreamID = writer.messageStreamID
	}
	writer.basicHeader.fmt = fmt
}

func (cs *ChunkStreamer) waitWriters() {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// Wait until that writers are finished for 3 seconds. (NOTE: 3s is addhoc value...)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for k, writer := range cs.writers {
		if err := writer.Wait(ctx); err != nil {
			cs.logger.Warnf("Failed to wait writer: ID = %d", k)
		}
	}
}

func (cs *ChunkStreamer) forceCloseWriters() {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	for _, writer := range cs.writers {
		//writer.lastErr = cs.err
		close(writer.closeCh)
	}
}

func (cs *ChunkStreamer) schedWriteLoop() {
	defer close(cs.done)
	cs.err = cs.writerSched.Run()

	if cs.err != nil {
		cs.forceCloseWriters()
	}
}

func (cs *ChunkStreamer) prepareChunkReader(chunkStreamID int) (*ChunkStreamReader, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	reader, ok := cs.readers[chunkStreamID]
	if !ok {
		if len(cs.readers) >= cs.config.MaxChunkStreams {
			return nil, errors.Errorf(
				"Creating chunk streams limit exceeded(Reader): Limit = %d",
				cs.config.MaxChunkStreams,
			)
		}

		reader = &ChunkStreamReader{}
		cs.readers[chunkStreamID] = reader
	}

	return reader, nil
}

func (cs *ChunkStreamer) prepareChunkWriter(chunkStreamID int) (*ChunkStreamWriter, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	writer, ok := cs.writers[chunkStreamID]
	if !ok {
		if len(cs.writers) >= cs.config.MaxChunkStreams {
			return nil, errors.Errorf(
				"Creating chunk streams limit exceeded(Writer): Limit = %d",
				cs.config.MaxChunkStreams,
			)
		}

		writer = &ChunkStreamWriter{
			ChunkStreamReader: ChunkStreamReader{
				basicHeader: chunkBasicHeader{
					chunkStreamID: chunkStreamID,
				},
				messageHeader: chunkMessageHeader{
					timestamp: math.MaxUint32, // initial state will be updated by writer.timestamp
				},
			},
			doneCh:   make(chan struct{}),
			closeCh:  make(chan struct{}),
			newChunk: true,
		}
		close(writer.doneCh)
		cs.writers[chunkStreamID] = writer
	}

	return writer, nil
}

func (cs *ChunkStreamer) sendAck(readBytes uint32) error {
	cs.logger.Debugf("Sending Ack...: Bytes = %d", readBytes)
	// TODO: fix timestamp
	return cs.controlStreamWriter(ctrlMsgChunkStreamID, 0, &message.Ack{
		SequenceNumber: readBytes,
	})
}

// handleAbort runs on the scheduler goroutine and performs a message abort.
// It marks the in-flight message as failed, drops its remaining chunks, notifies
// the peer with an AbortMessage and re-arms the writer for the next message.
func (cs *ChunkStreamer) handleAbort(req abortRequest) {
	defer close(req.replyCh)

	cs.mu.Lock()
	writer, ok := cs.writers[req.chunkStreamID]
	cs.mu.Unlock()
	if !ok {
		// Nothing was ever written on this chunk stream: nothing to abort.
		return
	}

	// Serialize against waiters of this writer while re-arming completion.
	writer.aqM.Lock()
	defer writer.aqM.Unlock()

	// Only a message that is still in flight can be aborted. Aborting an idle
	// chunk stream is a no-op and emits nothing.
	writer.stateM.Lock()
	if !writer.inFlight {
		writer.stateM.Unlock()
		return
	}
	writer.stateM.Unlock()

	// Drop the unfinished message and reset header state so that the next
	// message on this chunk stream starts with a fresh type-0 header.
	writer.resetForNewMessage()

	if err := cs.sendAbortMessage(uint32(req.chunkStreamID)); err != nil {
		cs.logger.Errorf("Failed to send AbortMessage: ChunkStreamID = %d, Err = %+v", req.chunkStreamID, err)
	}

	// Surface a clear failure to whoever is waiting for the aborted message.
	// Bumping the epoch invalidates every stale entry still sitting in the
	// scheduler queue. The completion channel is left closed (NOT re-armed here):
	// the next Wait observes and consumes this abort, clears the error and
	// re-arms the channel itself, allowing the chunk stream to be reused.
	writer.stateM.Lock()
	writer.inFlight = false
	writer.epoch++
	writer.stateM.Unlock()
	writer.lastErr = ErrMessageAborted
	close(writer.doneCh)
}

// sendAbortMessage writes an RTMP AbortMessage (type id 2) on the control chunk
// stream. It is emitted directly from the scheduler goroutine, therefore it does
// not contend with ordinary chunk output and works even before the control
// stream callback is wired up.
func (cs *ChunkStreamer) sendAbortMessage(chunkStreamID uint32) error {
	cs.logger.Debugf("Sending AbortMessage...: ChunkStreamID = %d", chunkStreamID)

	// Prefer the control-stream callback when a full connection is available so
	// that protocol control messages share the standard write path.
	if cs.controlStreamWriter != nil {
		return cs.controlStreamWriter(ctrlMsgChunkStreamID, 0, &message.AbortMessage{
			ChunkStreamID: chunkStreamID,
		})
	}

	return cs.writeControlMessage(&message.AbortMessage{
		ChunkStreamID: chunkStreamID,
	})
}

// writeControlMessage serializes a protocol control message into a single chunk
// on the control chunk stream and flushes it. Used only from the scheduler
// goroutine (abort handling) where no other write is in progress.
func (cs *ChunkStreamer) writeControlMessage(msg message.Message) error {
	writer, err := cs.prepareChunkWriter(ctrlMsgChunkStreamID)
	if err != nil {
		return errors.Wrapf(err, "Failed to prepare control writer")
	}

	writer.buf.Reset()
	cs.msgEnc.Reset(writer)
	if err := cs.msgEnc.Encode(msg); err != nil {
		return err
	}

	writer.timestamp = 0
	writer.messageLength = uint32(writer.buf.Len())
	writer.messageTypeID = byte(msg.TypeID())
	writer.messageStreamID = ControlStreamID
	writer.newChunk = true

	// Control messages are small: keep flushing until the single chunk is sent.
	for {
		completed, err := cs.writeChunk(writer)
		if err != nil {
			return err
		}
		if completed {
			return cs.w.Flush()
		}
	}
}

type chunkStreamerWriterSched struct {
	streamer *ChunkStreamer
	writers  chan scheduledWriter
	abortCh  chan abortRequest
	stopCh   chan struct{}
}

func (sched *chunkStreamerWriterSched) Sched(entry scheduledWriter) error {
	sched.writers <- entry

	return nil
}

func (sched *chunkStreamerWriterSched) Run() (err error) {
	defer func() {
		if r := recover(); r != nil {
			errTmp, ok := r.(error)
			if !ok {
				errTmp = errors.Errorf("Panic: %+v", r)
			}
			err = errors.WithStack(errTmp)
		}
	}()

	for {
		select {
		case req := <-sched.abortCh:
			sched.streamer.handleAbort(req)

		case entry := <-sched.writers:
			writer := entry.writer

			// Drop entries left over from an aborted (or superseded) message so
			// that no stale chunk of an older message reaches the wire. The
			// shared scheduling state is read under stateM to stay race-free
			// with Sched/Wait/handleAbort running on other goroutines.
			writer.stateM.Lock()
			stale := entry.epoch != writer.epoch || !writer.inFlight
			currentEpoch := writer.epoch
			writer.stateM.Unlock()
			if stale {
				continue
			}

			isCompleted, err := sched.streamer.writeChunk(writer)
			if err != nil {
				writer.stateM.Lock()
				writer.inFlight = false
				writer.stateM.Unlock()
				writer.lastErr = err
				close(writer.doneCh)
				return err
			}
			if isCompleted {
				writer.stateM.Lock()
				writer.inFlight = false
				writer.stateM.Unlock()
				close(writer.doneCh)
				continue
			}

			// Enqueue writer again, preserving the current epoch.
			sched.writers <- scheduledWriter{
				writer: writer,
				epoch:  currentEpoch,
			}

		case <-sched.stopCh:
			return nil
		}
	}
}

func (sched *chunkStreamerWriterSched) Close() error {
	close(sched.stopCh)

	return nil
}
