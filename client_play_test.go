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
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/yutopp/go-rtmp/message"
)

// recordingHandler records the media and metadata delivered to a playing
// client, preserving the received timestamps.
type recordingHandler struct {
	DefaultHandler

	mu         sync.Mutex
	audio      []timedPayload
	video      []timedPayload
	dataFrames []uint32
	unknownMsg int
	closed     bool
	closeCh    chan struct{}
}

type timedPayload struct {
	timestamp uint32
	payload   []byte
}

func newRecordingHandler() *recordingHandler {
	return &recordingHandler{closeCh: make(chan struct{})}
}

func (h *recordingHandler) OnAudio(timestamp uint32, payload io.Reader) error {
	buf, err := io.ReadAll(payload)
	if err != nil {
		return err
	}
	h.mu.Lock()
	h.audio = append(h.audio, timedPayload{timestamp: timestamp, payload: buf})
	h.mu.Unlock()
	return nil
}

func (h *recordingHandler) OnVideo(timestamp uint32, payload io.Reader) error {
	buf, err := io.ReadAll(payload)
	if err != nil {
		return err
	}
	h.mu.Lock()
	h.video = append(h.video, timedPayload{timestamp: timestamp, payload: buf})
	h.mu.Unlock()
	return nil
}

func (h *recordingHandler) OnSetDataFrame(timestamp uint32, data *message.NetStreamSetDataFrame) error {
	h.mu.Lock()
	h.dataFrames = append(h.dataFrames, timestamp)
	h.mu.Unlock()
	return nil
}

func (h *recordingHandler) OnUnknownMessage(timestamp uint32, msg message.Message) error {
	h.mu.Lock()
	h.unknownMsg++
	h.mu.Unlock()
	return nil
}

func (h *recordingHandler) OnClose() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.closed {
		h.closed = true
		close(h.closeCh)
	}
}

func (h *recordingHandler) snapshot() (audio, video []timedPayload, dataFrames []uint32) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]timedPayload(nil), h.audio...),
		append([]timedPayload(nil), h.video...),
		append([]uint32(nil), h.dataFrames...)
}

// playTestServerHandler accepts play requests and, from OnPlay, pushes
// metadata / audio / video messages with a fixed timeline back to the client.
// Writes happen synchronously on the server message-loop goroutine so they
// are serialized with the framework's own writes (ChunkStreamer is not safe
// for concurrent use), and therefore media may arrive before the Play.Start
// notification, exercising the client's optimistic play state.
type playTestServerHandler struct {
	DefaultHandler

	conn *Conn

	// pushCh is closed once the media burst has been written.
	pushCh chan struct{}
}

func (h *playTestServerHandler) OnServe(conn *Conn) {
	h.conn = conn
}

func (h *playTestServerHandler) writeOn(streamID uint32, csID int, timestamp uint32, msg message.Message) error {
	return h.conn.Write(context.Background(), csID, timestamp, &ChunkMessage{
		StreamID: streamID,
		Message:  msg,
	})
}

func (h *playTestServerHandler) OnPlay(ctx *StreamContext, timestamp uint32, cmd *message.NetStreamPlay) error {
	defer close(h.pushCh)

	// Metadata first, then audio and video interleaved on the original
	// timeline.
	amdBuf := new(bytes.Buffer)
	amfEnc := message.NewAMFEncoder(amdBuf, message.EncodingTypeAMF0)
	if err := message.EncodeBodyAnyValues(amfEnc, &message.NetStreamSetDataFrame{
		AmfData: map[string]interface{}{"duration": 1.0},
	}); err != nil {
		return err
	}
	_ = h.writeOn(ctx.StreamID, 8, 0, &message.DataMessage{
		Name:     "onMetaData",
		Encoding: message.EncodingTypeAMF0,
		Body:     amdBuf,
	})

	_ = h.writeOn(ctx.StreamID, 5, 0, &message.AudioMessage{
		Payload: bytes.NewReader([]byte{0xaf, 0x00}),
	})
	_ = h.writeOn(ctx.StreamID, 6, 40, &message.VideoMessage{
		Payload: bytes.NewReader([]byte{0x17, 0x01, 0x02, 0x03}),
	})
	_ = h.writeOn(ctx.StreamID, 5, 23, &message.AudioMessage{
		Payload: bytes.NewReader([]byte{0xaf, 0x01}),
	})

	return nil
}

func preparePlayConnection(t *testing.T, serverHandler, clientHandler Handler, f func(c *ClientConn)) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:")
	require.Nil(t, err)

	srv := NewServer(&ServerConfig{
		OnConnect: func(conn net.Conn) (io.ReadWriteCloser, *ConnConfig) {
			return conn, &ConnConfig{
				Handler: serverHandler,
				Logger:  logrus.StandardLogger(),
			}
		},
	})
	defer func() {
		_ = srv.Close()
	}()

	go func() {
		_ = srv.Serve(l)
	}()

	c, err := Dial("rtmp", l.Addr().String(), &ConnConfig{
		Handler: clientHandler,
		Logger:  logrus.StandardLogger(),
	})
	require.Nil(t, err)
	defer func() {
		_ = c.Close()
	}()

	f(c)
}

func TestClientPlayReceivesMediaOnOriginalTimeline(t *testing.T) {
	serverH := &playTestServerHandler{pushCh: make(chan struct{})}
	clientH := newRecordingHandler()

	preparePlayConnection(t, serverH, clientH, func(c *ClientConn) {
		require.Nil(t, c.Connect(nil))

		stream, err := c.CreateStream(nil, chunkSize)
		require.Nil(t, err)

		status, err := stream.Play(&message.NetStreamPlay{
			StreamName: "test-stream",
			Start:      -2,
		})
		require.Nil(t, err)
		require.Equal(t, message.NetStreamOnStatusCodePlayStart, status.InfoObject.Code)

		// Wait for the server-side media burst to finish, then give the client
		// message loop a moment to dispatch the callbacks.
		<-serverH.pushCh
		require.Eventually(t, func() bool {
			audio, video, dataFrames := clientH.snapshot()
			return len(audio) == 2 && len(video) == 1 && len(dataFrames) == 1
		}, 3*time.Second, 10*time.Millisecond)

		audio, video, dataFrames := clientH.snapshot()

		// Metadata delivered with the stream timeline.
		require.Equal(t, []uint32{0}, dataFrames)

		// Audio messages keep their original timestamps and payloads.
		require.Equal(t, []timedPayload{
			{timestamp: 0, payload: []byte{0xaf, 0x00}},
			{timestamp: 23, payload: []byte{0xaf, 0x01}},
		}, audio)

		// Video message keeps its original timestamp and payload.
		require.Equal(t, []timedPayload{
			{timestamp: 40, payload: []byte{0x17, 0x01, 0x02, 0x03}},
		}, video)

		// The stream must be in the client play state.
		require.Equal(t, streamStateClientPlay, stream.handler.State())
	})
}

// rejectPlayServerHandler rejects every play request.
type rejectPlayServerHandler struct {
	DefaultHandler
}

func (h *rejectPlayServerHandler) OnPlay(_ *StreamContext, _ uint32, _ *message.NetStreamPlay) error {
	return errPlayRejected
}

var errPlayRejected = &playRejectError{}

type playRejectError struct{}

func (e *playRejectError) Error() string { return "play rejected by test server" }

func TestClientPlayRejectedReturnsError(t *testing.T) {
	preparePlayConnection(t, &rejectPlayServerHandler{}, newRecordingHandler(), func(c *ClientConn) {
		require.Nil(t, c.Connect(nil))

		stream, err := c.CreateStream(nil, chunkSize)
		require.Nil(t, err)

		_, err = stream.Play(&message.NetStreamPlay{StreamName: "missing"})
		require.Error(t, err)

		var rejected *PlayRejectedError
		require.ErrorAs(t, err, &rejected)
		require.Equal(t, message.NetStreamOnStatusCodePlayFailed, rejected.Status.InfoObject.Code)

		// Rejected stream returns to the inactive state so it can be cleaned
		// up or retried.
		require.Equal(t, streamStateClientInactive, stream.handler.State())
	})
}

// playCompleteServerHandler accepts play requests and remembers the stream so
// the test can drive a StreamEOF event over it.
type playCompleteServerHandler struct {
	DefaultHandler
	conn     *Conn
	accepted chan *StreamContext
}

func (h *playCompleteServerHandler) OnServe(conn *Conn) {
	h.conn = conn
}

func (h *playCompleteServerHandler) OnPlay(ctx *StreamContext, _ uint32, _ *message.NetStreamPlay) error {
	h.accepted <- ctx
	return nil
}

func TestClientPlayHandlesStreamEOF(t *testing.T) {
	serverH := &playCompleteServerHandler{accepted: make(chan *StreamContext, 1)}

	preparePlayConnection(t, serverH, newRecordingHandler(), func(c *ClientConn) {
		require.Nil(t, c.Connect(nil))

		stream, err := c.CreateStream(nil, chunkSize)
		require.Nil(t, err)

		_, err = stream.Play(&message.NetStreamPlay{StreamName: "vod"})
		require.Nil(t, err)

		ctx := <-serverH.accepted
		// Emulate a server that reports completion through a UserCtrl
		// StreamEOF event. The test goroutine is the only writer here besides
		// the framework, which has finished its Play.Start response.
		require.Nil(t, serverH.conn.Write(context.Background(), 2, 0, &ChunkMessage{
			StreamID: ctx.StreamID,
			Message: &message.UserCtrl{
				Event: &message.UserCtrlEventStreamEOF{StreamID: ctx.StreamID},
			},
		}))

		require.Eventually(t, func() bool {
			return stream.handler.State() == streamStateClientInactive
		}, 3*time.Second, 10*time.Millisecond)
	})
}

func TestClientPlayTimeoutWhenNoStatus(t *testing.T) {
	// Verify the timeout plumbing directly on a raw stream which never gets
	// an onStatus notification.
	rwc := &rwcMock{}
	conn := newConn(rwc, &ConnConfig{
		SkipHandshakeVerification: true,
	})
	s, err := conn.streams.Create(1)
	require.Nil(t, err)
	s.handler.ChangeState(streamStateClientInactive)

	_, err = s.PlayWithTimeout(&message.NetStreamPlay{StreamName: "x"}, 20*time.Millisecond)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Timed out")

	// After a timeout the stream returns to inactive and can be cleaned up.
	require.Equal(t, streamStateClientInactive, s.handler.State())
}

func TestClientCloseStreamRemovesLocalStream(t *testing.T) {
	serverH := &playTestServerHandler{pushCh: make(chan struct{})}

	preparePlayConnection(t, serverH, newRecordingHandler(), func(c *ClientConn) {
		require.Nil(t, c.Connect(nil))

		stream, err := c.CreateStream(nil, chunkSize)
		require.Nil(t, err)
		streamID := stream.StreamID()

		_, err = stream.Play(&message.NetStreamPlay{StreamName: "test-stream"})
		require.Nil(t, err)
		<-serverH.pushCh

		// Cleanup before reconnecting: the local stream entry must disappear.
		require.Nil(t, stream.CloseStream())
		_, err = c.conn.streams.At(streamID)
		require.Error(t, err)
	})
}

func TestClientPlayAndPublishStatesAreIndependent(t *testing.T) {
	serverH := &multiStreamServerHandler{}
	clientH := &isolationRecordingHandler{}

	preparePlayConnection(t, serverH, clientH, func(c *ClientConn) {
		require.Nil(t, c.Connect(nil))

		playStream, err := c.CreateStream(nil, chunkSize)
		require.Nil(t, err)
		pubStream, err := c.CreateStream(nil, chunkSize)
		require.Nil(t, err)

		_, err = playStream.Play(&message.NetStreamPlay{StreamName: "stream-a"})
		require.Nil(t, err)
		require.Nil(t, pubStream.Publish(&message.NetStreamPublish{
			PublishingName: "out",
			PublishingType: "live",
		}))

		// Playing and publishing states coexist on the same connection without
		// interfering with each other.
		require.Equal(t, streamStateClientPlay, playStream.handler.State())
		require.Equal(t, streamStateClientPublish, pubStream.handler.State())
		require.NotSame(t, playStream.statusCh, pubStream.statusCh)
	})
}
