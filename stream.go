//
// Copyright (c) 2018- yutopp (yutopp@gmail.com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at  https://www.boost.org/LICENSE_1_0.txt)
//

package rtmp

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/yutopp/go-rtmp/message"
)

// Stream represents a logical message stream
type Stream struct {
	streamID     uint32
	encTy        message.EncodingType
	transactions *transactions
	handler      *streamHandler
	cmsg         ChunkMessage

	conn *Conn

	// statusCh delivers onStatus notifications received on this stream.
	// It is buffered so that the message loop never blocks even when nobody
	// is waiting (for example while publishing). Every stream owns its own
	// channel, therefore notifications of multiple streams on the same
	// connection can never be mixed.
	statusCh  chan *message.NetStreamOnStatus
	closeOnce sync.Once
}

func newStream(streamID uint32, conn *Conn) *Stream {
	s := &Stream{
		streamID:     streamID,
		encTy:        message.EncodingTypeAMF0, // Default AMF encoding type
		transactions: newTransactions(),
		cmsg: ChunkMessage{
			StreamID: streamID,
		},

		conn: conn,

		statusCh: make(chan *message.NetStreamOnStatus, 16),
	}
	s.handler = newStreamHandler(s)

	return s
}

func (s *Stream) StreamID() uint32 {
	return s.streamID
}

func (s *Stream) WriteWinAckSize(chunkStreamID int, timestamp uint32, msg *message.WinAckSize) error {
	return s.Write(chunkStreamID, timestamp, msg)
}

func (s *Stream) WriteSetPeerBandwidth(chunkStreamID int, timestamp uint32, msg *message.SetPeerBandwidth) error {
	return s.Write(chunkStreamID, timestamp, msg)
}

func (s *Stream) WriteUserCtrl(chunkStreamID int, timestamp uint32, msg *message.UserCtrl) error {
	return s.Write(chunkStreamID, timestamp, msg)
}

func (s *Stream) Connect(
	body *message.NetConnectionConnect,
) (*message.NetConnectionConnectResult, error) {
	transactionID := int64(1) // Always 1 (7.2.1.1)
	t, err := s.transactions.Create(transactionID)
	if err != nil {
		return nil, err
	}

	if body == nil {
		body = &message.NetConnectionConnect{}
	}

	chunkStreamID := 3 // TODO: fix
	err = s.writeCommandMessage(
		chunkStreamID, 0, // Timestamp is 0
		"connect",
		transactionID,
		body,
	)
	if err != nil {
		return nil, err
	}

	// TODO: support timeout
	timeoutCtx := context.TODO()
	select {
	case <-timeoutCtx.Done():
		return nil, timeoutCtx.Err()
	case <-t.doneCh:
		amfDec := message.NewAMFDecoder(t.body, t.encoding)

		var value message.AMFConvertible
		if err := message.DecodeBodyConnectResult(t.body, amfDec, &value); err != nil {
			return nil, errors.Wrap(err, "Failed to decode result")
		}
		result := value.(*message.NetConnectionConnectResult)

		if t.commandName == "_error" {
			return nil, &ConnectRejectedError{
				TransactionID: transactionID,
				Result:        result,
			}
		}

		return result, nil
	}

	//return nil, errors.New("Failed to get result")
}

func (s *Stream) ReplyConnect(
	chunkStreamID int,
	timestamp uint32,
	body *message.NetConnectionConnectResult,
) error {
	var commandName string
	switch body.Information.Code {
	case message.NetConnectionConnectCodeSuccess, message.NetConnectionConnectCodeClosed:
		commandName = "_result"
	case message.NetConnectionConnectCodeFailed:
		commandName = "_error"
	}

	return s.writeCommandMessage(
		chunkStreamID, timestamp,
		commandName,
		1, // 7.2.1.2, flow.6
		body,
	)
}

func (s *Stream) CreateStream(body *message.NetConnectionCreateStream, chunkSize uint32) (*message.NetConnectionCreateStreamResult, error) {
	oldChunkSize := s.conn.streamer.selfState.chunkSize
	if chunkSize > 0 && chunkSize != oldChunkSize {
		logrus.Infof("Changing chunkSize %d->%d", oldChunkSize, chunkSize)
		s.conn.streamer.selfState.chunkSize = chunkSize
		err := s.WriteSetChunkSize(chunkSize)
		if err != nil {
			return nil, err
		}
	}

	transactionID := int64(2) // TODO: fix
	t, err := s.transactions.Create(transactionID)
	if err != nil {
		return nil, err
	}

	if body == nil {
		body = &message.NetConnectionCreateStream{}
	}

	chunkStreamID := 3 // TODO: fix
	err = s.writeCommandMessage(
		chunkStreamID, 0, // TODO: fix, Timestamp is 0
		"createStream",
		transactionID,
		body,
	)
	if err != nil {
		return nil, err
	}

	// TODO: support timeout
	// TODO: check result
	timeoutCtx := context.TODO()
	select {
	case <-timeoutCtx.Done():
		return nil, timeoutCtx.Err()
	case <-t.doneCh:
		amfDec := message.NewAMFDecoder(t.body, t.encoding)

		var value message.AMFConvertible
		if err := message.DecodeBodyCreateStreamResult(t.body, amfDec, &value); err != nil {
			return nil, errors.Wrap(err, "Failed to decode result")
		}
		result := value.(*message.NetConnectionCreateStreamResult)

		if t.commandName == "_error" {
			return nil, &CreateStreamRejectedError{
				TransactionID: transactionID,
				Result:        result,
			}
		}

		return result, nil
	}

	//return nil, errors.New("Failed to get result")
}

func (s *Stream) DeleteStream(body *message.NetStreamDeleteStream) error {
	chunkStreamID := 3 // TODO: fix

	return s.writeCommandMessage(
		chunkStreamID,
		0,
		"deleteStream",
		0,
		body,
	)
}

func (s *Stream) ReplyCreateStream(
	chunkStreamID int,
	timestamp uint32,
	transactionID int64,
	body *message.NetConnectionCreateStreamResult,
) error {
	commandName := "_result"
	if body == nil {
		commandName = "_error"
		body = &message.NetConnectionCreateStreamResult{
			StreamID: 0, // TODO: Change to error information object...
		}
	}

	return s.writeCommandMessage(
		chunkStreamID, timestamp,
		commandName,
		transactionID,
		body,
	)
}

func (s *Stream) Publish(
	body *message.NetStreamPublish,
) error {
	if body == nil {
		body = &message.NetStreamPublish{}
	}

	// A freshly created data stream is in the client-inactive state. Move it
	// to the publishing state before the command is sent so that the server's
	// onStatus response is handled by the publish handler.
	if s.handler.State() == streamStateClientInactive {
		s.handler.ChangeState(streamStateClientPublish)
	}

	chunkStreamID := 3 // TODO: fix
	return s.writeCommandMessage(
		chunkStreamID, 0, // TODO: fix, Timestamp is 0
		"publish",
		int64(0), // Always 0, 7.2.2.6
		body,
	)
}

// DefaultPlayStatusTimeout is the maximum time Play waits for the server's
// start/failed onStatus notification.
const DefaultPlayStatusTimeout = 30 * time.Second

// Play sends a play request for body.StreamName on this message stream and
// blocks until the server accepts or rejects the request.
// On success the stream is in the playing state and audio, video and metadata
// messages are delivered, with their original timestamps, through the Handler
// callbacks until the stream ends or the connection is closed.
func (s *Stream) Play(body *message.NetStreamPlay) (*message.NetStreamOnStatus, error) {
	return s.PlayWithTimeout(body, DefaultPlayStatusTimeout)
}

// PlayWithTimeout is Play with an explicit deadline for waiting on the
// server's play status notification. A timeout of zero disables the deadline.
func (s *Stream) PlayWithTimeout(
	body *message.NetStreamPlay,
	timeout time.Duration,
) (*message.NetStreamOnStatus, error) {
	if body == nil {
		body = &message.NetStreamPlay{}
	}

	// Enter the inactive state first so that the response to the play command
	// is routed to a handler which understands onStatus messages.
	if s.handler.State() != streamStateClientInactive &&
		s.handler.State() != streamStateClientPlay {
		s.handler.ChangeState(streamStateClientInactive)
	}

	// Discard notifications left over from a previous (possibly timed out)
	// attempt so that this call waits for its own response.
	s.drainPlayStatus()

	chunkStreamID := 4 // Data/commands of a playing stream use chunk stream 4
	if err := s.writeCommandMessage(
		chunkStreamID, 0, // Timestamp is 0 for commands
		"play",
		int64(0), // Always 0, 7.2.2.1
		body,
	); err != nil {
		return nil, err
	}

	// Optimistically enter the playing state. A server is allowed to start
	// delivering media before (or without) the Play.Start onStatus
	// notification, therefore the stream must already route audio, video and
	// metadata to the play handler. On rejection the state is rolled back.
	s.handler.ChangeState(streamStateClientPlay)

	status, err := s.waitPlayStatus(timeout)
	if err != nil {
		s.handler.ChangeState(streamStateClientInactive)
		return nil, err
	}

	if status.InfoObject.Level == message.NetStreamOnStatusLevelError ||
		status.InfoObject.Code == message.NetStreamOnStatusCodePlayFailed ||
		status.InfoObject.Code == message.NetStreamOnStatusCodeConnectFailed {
		// Rejected: return to inactive so that callers can retry on a fresh
		// stream after cleaning up.
		s.handler.ChangeState(streamStateClientInactive)
		return nil, &PlayRejectedError{
			StreamID: s.streamID,
			Status:   status,
		}
	}

	// Accepted: media is delivered on this same message stream. Make sure the
	// play state is active even when the handler was changed concurrently.
	s.handler.ChangeState(streamStateClientPlay)

	return status, nil
}

// CloseStream sends closeStream to release the server-side resources bound to
// this message stream and removes the stream locally. It is the cleanup step
// required before reconnecting or reusing the stream name. Local cleanup is
// performed even when the network write fails (for example a dead connection).
func (s *Stream) CloseStream() error {
	chunkStreamID := 4

	writeErr := s.writeCommandMessage(
		chunkStreamID, 0,
		"closeStream",
		int64(0),
		&message.NetStreamCloseStream{},
	)

	if err := s.conn.streams.Delete(s.streamID); err != nil && writeErr == nil {
		return err
	}

	return writeErr
}

// notifyPlayStatus enqueues an onStatus notification for this stream. The
// message loop calls it; Play consumes the notification via waitPlayStatus.
func (s *Stream) notifyPlayStatus(status *message.NetStreamOnStatus) {
	defer func() {
		// The channel can be closed concurrently when the stream is deleted
		// (CloseStream). A late notification is simply discarded in that case.
		_ = recover()
	}()

	select {
	case s.statusCh <- status:
	default:
		// Drop the oldest notification rather than blocking the message loop
		// when the buffer is full.
		select {
		case <-s.statusCh:
		default:
		}
		s.statusCh <- status
	}
}

// handlePlayPublishStatus is the single entry point used by every client data
// handler when an onStatus command is received.
func (s *Stream) handlePlayPublishStatus(status *message.NetStreamOnStatus) error {
	s.notifyPlayStatus(status)
	return nil
}

func (s *Stream) waitPlayStatus(timeout time.Duration) (*message.NetStreamOnStatus, error) {
	var timeoutCh <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		timeoutCh = timer.C
	}

	select {
	case status, ok := <-s.statusCh:
		if !ok {
			return nil, errors.New("Stream is closed while waiting for play status")
		}
		return status, nil
	case <-s.conn.Done():
		return nil, errors.New("Connection is closed while waiting for play status")
	case <-timeoutCh:
		return nil, errors.Errorf("Timed out waiting for play status: Timeout = %s", timeout)
	}
}

// drainPlayStatus removes any buffered notifications. It never blocks and
// tolerates a closed channel.
func (s *Stream) drainPlayStatus() {
	for {
		select {
		case _, ok := <-s.statusCh:
			if !ok {
				return
			}
		default:
			return
		}
	}
}

func (s *Stream) NotifyStatus(
	chunkStreamID int,
	timestamp uint32,
	body *message.NetStreamOnStatus,
) error {
	return s.writeCommandMessage(
		chunkStreamID, timestamp,
		"onStatus",
		0, // 7.2.2
		body,
	)
}

func (s *Stream) Close() error {
	s.assumeClosed()
	return nil // TODO: implement
}

func (s *Stream) assumeClosed() {
	if s == nil || s.conn == nil {
		return
	}

	// Unblock Play which may be waiting for a status notification that will
	// never arrive once the stream is gone.
	s.closeOnce.Do(func() {
		close(s.statusCh)
	})
}

func (s *Stream) writeCommandMessage(
	chunkStreamID int,
	timestamp uint32,
	commandName string,
	transactionID int64,
	body message.AMFConvertible,
) error {
	buf := new(bytes.Buffer)
	amfEnc := message.NewAMFEncoder(buf, s.encTy)
	if err := message.EncodeBodyAnyValues(amfEnc, body); err != nil {
		return err
	}

	return s.Write(chunkStreamID, timestamp, &message.CommandMessage{
		CommandName:   commandName,
		TransactionID: transactionID,
		Encoding:      s.encTy,
		Body:          buf,
	})
}

func (s *Stream) WriteDataMessage(
	chunkStreamID int,
	timestamp uint32,
	name string,
	body message.AMFConvertible,
) error {
	buf := new(bytes.Buffer)
	amfEnc := message.NewAMFEncoder(buf, message.EncodingTypeAMF0)
	if err := message.EncodeBodyAnyValues(amfEnc, body); err != nil {
		return err
	}

	return s.Write(chunkStreamID, timestamp, &message.DataMessage{
		Name:     name,
		Encoding: message.EncodingTypeAMF0,
		Body:     buf,
	})
}

func (s *Stream) WriteSetChunkSize(chunkSize uint32) error {
	if chunkSize < 1 {
		return errors.New("chunksize < 1")
	}
	if chunkSize > 0x7fffffff {
		return errors.New("chunksize > 0x7fffffff")
	}
	msg := &message.SetChunkSize{ChunkSize: chunkSize}
	chunkStreamID := 2       // Correct according to 6.2
	var timeStamp uint32 = 0 // TODO. Send updated time
	return s.Write(chunkStreamID, timeStamp, msg)
}

func (s *Stream) Write(chunkStreamID int, timestamp uint32, msg message.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // TODO: Fix 5s
	defer cancel()

	s.cmsg.Message = msg
	return s.streamer().Write(ctx, chunkStreamID, timestamp, &s.cmsg)
}

func (s *Stream) handle(chunkStreamID int, timestamp uint32, msg message.Message) error {
	return s.handler.Handle(chunkStreamID, timestamp, msg)
}

func (s *Stream) streams() *streams {
	return s.conn.streams
}

func (s *Stream) streamer() *ChunkStreamer {
	return s.conn.streamer
}

func (s *Stream) userHandler() Handler {
	return s.conn.handler
}

func (s *Stream) logger() logrus.FieldLogger {
	return s.conn.logger
}
