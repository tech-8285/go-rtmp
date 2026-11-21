//
// Copyright (c) 2018- yutopp (yutopp@gmail.com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at  https://www.boost.org/LICENSE_1_0.txt)
//

package rtmp

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/yutopp/go-rtmp/message"
)

// pacedWriter delegates every write to inner and reports once the first chunk
// body has been emitted. It does not block the scheduler: tests abort right
// after the first chunk lands, and because the payload spans thousands of
// chunks the message is guaranteed to still be fragmented at that instant.
type pacedWriter struct {
	inner io.Writer

	firstChunkOnce sync.Once
	firstChunkCh   chan struct{}
}

func newPacedWriter(inner io.Writer) *pacedWriter {
	return &pacedWriter{
		inner:        inner,
		firstChunkCh: make(chan struct{}),
	}
}

func (w *pacedWriter) Write(p []byte) (int, error) {
	n, err := w.inner.Write(p)
	// A chunk body write is large (the chunk size); header writes are tiny.
	if len(p) >= 128 {
		w.firstChunkOnce.Do(func() { close(w.firstChunkCh) })
	}
	return n, err
}

func (w *pacedWriter) waitFirstChunk(t *testing.T) {
	t.Helper()
	select {
	case <-w.firstChunkCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the first chunk to be emitted")
	}
}

// mustFindAbortMessage builds a short-lived read streamer, locates the first
// completed AbortMessage and tears the streamer down before returning, so no
// scheduler goroutine is left behind.
func mustFindAbortMessage(t *testing.T, buf *bytes.Buffer) *message.AbortMessage {
	t.Helper()
	r, closeFn := newReadStreamer(buf)
	defer closeFn()
	return findAbortMessage(t, r)
}

// newReadStreamer builds a read-only ChunkStreamer over the given bytes. The
// returned close function must be invoked before the test finishes so that its
// scheduler goroutine does not leak.
func newReadStreamer(buf *bytes.Buffer) (*ChunkStreamer, func()) {
	r := NewChunkStreamer(bytes.NewReader(buf.Bytes()), nil, nil)
	return r, func() {
		_ = r.Close()
		<-r.Done()
	}
}

func buildChunkWriter(t *testing.T, streamer *ChunkStreamer, chunkStreamID int, payload []byte) *ChunkStreamWriter {
	t.Helper()

	w, err := streamer.NewChunkWriter(context.Background(), chunkStreamID)
	require.NoError(t, err)

	msg := &message.VideoMessage{Payload: bytes.NewReader(payload)}
	enc := message.NewEncoder(w)
	require.NoError(t, enc.Encode(msg))

	w.messageLength = uint32(w.buf.Len())
	w.messageTypeID = byte(msg.TypeID())
	w.messageStreamID = 1
	w.timestamp = 100
	return w
}

// startAbortableWrite schedules a large, fragmented message and returns once
// its first chunk is on the wire while thousands of chunks remain queued.
func startAbortableWrite(t *testing.T, streamer *ChunkStreamer, chunkStreamID int) *pacedWriter {
	t.Helper()

	pw := newPacedWriter(streamer.w.writer)
	streamer.w.writer = pw

	// 64 KiB payload at the 128-byte default chunk size => 512 chunks, so the
	// message cannot finish before the test gets a chance to abort it.
	largePayload := []byte(strings.Repeat("abcd", 16*1024))
	require.NoError(t, streamer.Sched(buildChunkWriter(t, streamer, chunkStreamID, largePayload)))

	pw.waitFirstChunk(t)
	return pw
}

// findAbortMessage walks raw chunks and returns the first completed
// AbortMessage. Unlike NewChunkReader it does not consume aborts internally, so
// tests can assert that the notification was actually put on the wire. The
// partially-buffered message named by the abort is discarded, mirroring the
// production receive path.
func findAbortMessage(t *testing.T, r *ChunkStreamer) *message.AbortMessage {
	t.Helper()
	for {
		cr, err := r.readChunk()
		require.NoError(t, err)
		if !cr.completed || cr.messageTypeID != byte(message.TypeIDAbortMessage) {
			continue
		}
		var decoded message.Message
		require.NoError(t, message.NewDecoder(cr).Decode(message.TypeIDAbortMessage, &decoded))
		abort := decoded.(*message.AbortMessage)
		r.discardChunkReader(int(abort.ChunkStreamID))
		return abort
	}
}

// TestAbortSendsAbortMessageAndStopsRemainingChunks verifies that aborting an
// in-flight message stops its remaining chunks and emits an AbortMessage which
// references the aborted chunk stream.
func TestAbortSendsAbortMessageAndStopsRemainingChunks(t *testing.T) {
	defer leaktest.Check(t)()

	buf := new(bytes.Buffer)
	streamer := NewChunkStreamer(bufio.NewReader(buf), buf, nil)

	const chunkStreamID = 7
	_ = startAbortableWrite(t, streamer, chunkStreamID)

	// Abort while the large, fragmented message is still being chunked.
	require.NoError(t, streamer.Abort(chunkStreamID))

	// The wire must carry an AbortMessage naming the aborted chunk stream, and
	// no complete video message (its remaining chunks were never written).
	abort := mustFindAbortMessage(t, buf)
	require.Equal(t, uint32(chunkStreamID), abort.ChunkStreamID)

	// No further complete message may be delivered on the aborted chunk stream.
	emptyReader, closeEmpty := newReadStreamer(buf)
	defer closeEmpty()
	_, err := emptyReader.NewChunkReader()
	require.Error(t, err)

	require.NoError(t, streamer.Close())
	<-streamer.Done()
}

// TestAbortAllowsReuseOfSameChunkStream verifies that after an abort a fresh
// message can be sent on the same chunk stream and is fully delivered.
func TestAbortAllowsReuseOfSameChunkStream(t *testing.T) {
	defer leaktest.Check(t)()

	buf := new(bytes.Buffer)
	streamer := NewChunkStreamer(bufio.NewReader(buf), buf, nil)

	const chunkStreamID = 7
	_ = startAbortableWrite(t, streamer, chunkStreamID)
	require.NoError(t, streamer.Abort(chunkStreamID))

	// Send a new, small message on the very same chunk stream and wait for it
	// to be fully flushed.
	newContent := []byte("fresh-message")
	require.NoError(t, streamer.WriteWithDeadline(context.Background(), chunkStreamID, 200, &ChunkMessage{
		StreamID: 1,
		Message: &message.VideoMessage{
			Payload: bytes.NewReader(newContent),
		},
	}))
	require.NoError(t, streamer.Close())
	<-streamer.Done()
	require.NoError(t, streamer.Err())

	// The receiver must see the abort notification followed by the complete
	// new message, and never the truncated first message.

	// AbortMessage is present on the wire naming this chunk stream.
	abort := mustFindAbortMessage(t, buf)
	require.Equal(t, uint32(chunkStreamID), abort.ChunkStreamID)

	// The only deliverable application message is the complete, fresh one.
	reader, closeReader := newReadStreamer(buf)
	defer closeReader()
	r, err := reader.NewChunkReader()
	require.NoError(t, err)
	require.True(t, r.completed)
	var got message.Message
	require.NoError(t, message.NewDecoder(r).Decode(message.TypeID(r.messageTypeID), &got))
	gotContent, err := ioutil.ReadAll(got.(*message.VideoMessage).Payload)
	require.NoError(t, err)
	require.Equal(t, newContent, gotContent)
}

// TestWriteWithDeadlineExpiresAndAborts verifies the deadline path: when ctx is
// canceled before the message finished, WriteWithDeadline returns
// ErrMessageAborted and an AbortMessage is delivered to the peer.
func TestWriteWithDeadlineExpiresAndAborts(t *testing.T) {
	defer leaktest.Check(t)()

	buf := new(bytes.Buffer)
	streamer := NewChunkStreamer(bufio.NewReader(buf), buf, nil)
	pw := newPacedWriter(streamer.w.writer)
	streamer.w.writer = pw

	const chunkStreamID = 9
	largePayload := []byte(strings.Repeat("wxyz", 16*1024)) // 64 KiB, many chunks

	ctx, cancel := context.WithCancel(context.Background())
	doneCh := make(chan error, 1)
	go func() {
		doneCh <- streamer.WriteWithDeadline(ctx, chunkStreamID, 100, &ChunkMessage{
			StreamID: 1,
			Message:  &message.VideoMessage{Payload: bytes.NewReader(largePayload)},
		})
	}()

	// Wait until flushing has started, then expire the deadline while the
	// fragmented message is still being sent.
	pw.waitFirstChunk(t)
	cancel()
	select {
	case err := <-doneCh:
		require.True(t, errors.Is(err, ErrMessageAborted), "expected ErrMessageAborted, got %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("WriteWithDeadline did not return after deadline expired")
	}

	// The wire must carry the abort notification for this chunk stream.
	abort := mustFindAbortMessage(t, buf)
	require.Equal(t, uint32(chunkStreamID), abort.ChunkStreamID)

	require.NoError(t, streamer.Close())
	<-streamer.Done()
}

// TestWriteWithDeadlineCompletesBeforeExpiry verifies that a message finished
// before the deadline behaves like an ordinary write and reports success.
func TestWriteWithDeadlineCompletesBeforeExpiry(t *testing.T) {
	defer leaktest.Check(t)()

	buf := new(bytes.Buffer)
	streamer := NewChunkStreamer(bufio.NewReader(buf), buf, nil)

	content := []byte("on-time")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := streamer.WriteWithDeadline(ctx, 11, 300, &ChunkMessage{
		StreamID: 1,
		Message:  &message.VideoMessage{Payload: bytes.NewReader(content)},
	})
	require.NoError(t, err)
	require.NoError(t, streamer.Close())
	<-streamer.Done()

	reader, closeReader := newReadStreamer(buf)
	defer closeReader()
	r, err := reader.NewChunkReader()
	require.NoError(t, err)
	require.True(t, r.completed)
	var got message.Message
	require.NoError(t, message.NewDecoder(r).Decode(message.TypeID(r.messageTypeID), &got))
	gotContent, err := ioutil.ReadAll(got.(*message.VideoMessage).Payload)
	require.NoError(t, err)
	require.Equal(t, content, gotContent)
}

// TestReceiverDiscardsAbortedChunkStream verifies the receiving side never
// delivers a partial message after an AbortMessage names its chunk stream,
// while another chunk stream keeps working.
func TestReceiverDiscardsAbortedChunkStream(t *testing.T) {
	defer leaktest.Check(t)()

	buf := new(bytes.Buffer)
	sender := NewChunkStreamer(bufio.NewReader(buf), buf, nil)

	const abortedCSID = 4
	const otherCSID = 5

	// Schedule a large, fragmented message, then abort it.
	_ = startAbortableWrite(t, sender, abortedCSID)
	require.NoError(t, sender.Abort(abortedCSID))

	// A complete message on a different chunk stream must be unaffected.
	otherContent := []byte("other-stream-content")
	require.NoError(t, sender.WriteWithDeadline(context.Background(), otherCSID, 400, &ChunkMessage{
		StreamID: 1,
		Message:  &message.VideoMessage{Payload: bytes.NewReader(otherContent)},
	}))
	require.NoError(t, sender.Close())
	<-sender.Done()
	require.NoError(t, sender.Err())

	// Receiver: the AbortMessage is consumed transparently, the first delivered
	// message must be the one from the unaffected chunk stream.
	receiver, closeReceiver := newReadStreamer(buf)
	defer closeReceiver()
	r, err := receiver.NewChunkReader()
	require.NoError(t, err)
	require.Equal(t, otherCSID, r.basicHeader.chunkStreamID)
	require.True(t, r.completed)

	var got message.Message
	require.NoError(t, message.NewDecoder(r).Decode(message.TypeID(r.messageTypeID), &got))
	gotContent, err := ioutil.ReadAll(got.(*message.VideoMessage).Payload)
	require.NoError(t, err)
	require.Equal(t, otherContent, gotContent)

	// No truncated message from the aborted stream may surface afterwards.
	_, err = receiver.NewChunkReader()
	require.Error(t, err)
}

// TestAbortUnknownChunkStreamIsNoOp verifies aborting a chunk stream that has
// never been used does not error and does not emit anything.
func TestAbortUnknownChunkStreamIsNoOp(t *testing.T) {
	defer leaktest.Check(t)()

	buf := new(bytes.Buffer)
	streamer := NewChunkStreamer(bufio.NewReader(buf), buf, nil)

	require.NoError(t, streamer.Abort(123))
	require.NoError(t, streamer.Close())
	<-streamer.Done()
	require.NoError(t, streamer.Err())
	require.Equal(t, 0, buf.Len())
}
