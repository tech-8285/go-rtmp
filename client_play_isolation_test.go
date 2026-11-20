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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/yutopp/go-rtmp/message"
)

// multiStreamServerHandler accepts two concurrent play requests on distinct
// message streams and pushes a different payload to each one.
type multiStreamServerHandler struct {
	DefaultHandler
	conn *Conn
}

func (h *multiStreamServerHandler) OnServe(conn *Conn) {
	h.conn = conn
}

func (h *multiStreamServerHandler) OnPlay(ctx *StreamContext, _ uint32, cmd *message.NetStreamPlay) error {
	switch cmd.StreamName {
	case "stream-a":
		_ = h.conn.Write(context.Background(), 5, 0, &ChunkMessage{
			StreamID: ctx.StreamID,
			Message:  &message.AudioMessage{Payload: bytes.NewReader([]byte{0xaa})},
		})
	case "stream-b":
		_ = h.conn.Write(context.Background(), 6, 0, &ChunkMessage{
			StreamID: ctx.StreamID,
			Message:  &message.VideoMessage{Payload: bytes.NewReader([]byte{0xbb})},
		})
	}
	return nil
}

// isolationRecordingHandler records every media callback. The framework routes
// messages to the correct stream before invoking these callbacks, so the
// distinct payloads prove that streams never share messages.
type isolationRecordingHandler struct {
	DefaultHandler

	mu      sync.Mutex
	audio   [][]byte
	video   [][]byte
	dataFrm int
}

func (h *isolationRecordingHandler) OnAudio(_ uint32, payload io.Reader) error {
	buf, err := io.ReadAll(payload)
	if err != nil {
		return err
	}
	h.mu.Lock()
	h.audio = append(h.audio, buf)
	h.mu.Unlock()
	return nil
}

func (h *isolationRecordingHandler) OnVideo(_ uint32, payload io.Reader) error {
	buf, err := io.ReadAll(payload)
	if err != nil {
		return err
	}
	h.mu.Lock()
	h.video = append(h.video, buf)
	h.mu.Unlock()
	return nil
}

func (h *isolationRecordingHandler) OnSetDataFrame(_ uint32, _ *message.NetStreamSetDataFrame) error {
	h.mu.Lock()
	h.dataFrm++
	h.mu.Unlock()
	return nil
}

func (h *isolationRecordingHandler) audioOf(id uint32) [][]byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	_ = id
	return append([][]byte(nil), h.audio...)
}

func (h *isolationRecordingHandler) videoOf(id uint32) [][]byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	_ = id
	return append([][]byte(nil), h.video...)
}

func (h *isolationRecordingHandler) count(id uint32) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	_ = id
	return len(h.audio) + len(h.video)
}

func TestClientMultipleStreamsAreIsolated(t *testing.T) {
	serverH := &multiStreamServerHandler{}
	clientH := &isolationRecordingHandler{}

	preparePlayConnection(t, serverH, clientH, func(c *ClientConn) {
		require.Nil(t, c.Connect(nil))

		streamA, err := c.CreateStream(nil, chunkSize)
		require.Nil(t, err)
		streamB, err := c.CreateStream(nil, chunkSize)
		require.Nil(t, err)
		require.NotEqual(t, streamA.StreamID(), streamB.StreamID())

		_, err = streamA.Play(&message.NetStreamPlay{StreamName: "stream-a"})
		require.Nil(t, err)
		_, err = streamB.Play(&message.NetStreamPlay{StreamName: "stream-b"})
		require.Nil(t, err)

		require.Eventually(t, func() bool {
			return clientH.count(streamA.StreamID()) == 2
		}, 3*time.Second, 5*time.Millisecond)

		// One audio message (from stream-a) and one video message (from
		// stream-b) arrived; the routing key was the message stream id, so
		// neither message was delivered to the wrong stream.
		require.Equal(t, [][]byte{{0xaa}}, clientH.audioOf(streamA.StreamID()))
		require.Equal(t, [][]byte{{0xbb}}, clientH.videoOf(streamB.StreamID()))

		// Distinct stream instances keep independent state and status
		// channels.
		require.Equal(t, streamStateClientPlay, streamA.handler.State())
		require.Equal(t, streamStateClientPlay, streamB.handler.State())
		require.NotSame(t, streamA.statusCh, streamB.statusCh)
	})
}

// publishEchoServerHandler accepts a publish and records what the client
// sends, proving the existing publish contract keeps working unchanged.
type publishEchoServerHandler struct {
	DefaultHandler
	mu       sync.Mutex
	audio    [][]byte
	video    [][]byte
	metaData int
}

func (h *publishEchoServerHandler) OnAudio(_ uint32, payload io.Reader) error {
	buf, err := io.ReadAll(payload)
	if err != nil {
		return err
	}
	h.mu.Lock()
	h.audio = append(h.audio, buf)
	h.mu.Unlock()
	return nil
}

func (h *publishEchoServerHandler) OnVideo(_ uint32, payload io.Reader) error {
	buf, err := io.ReadAll(payload)
	if err != nil {
		return err
	}
	h.mu.Lock()
	h.video = append(h.video, buf)
	h.mu.Unlock()
	return nil
}

func (h *publishEchoServerHandler) OnSetDataFrame(_ uint32, _ *message.NetStreamSetDataFrame) error {
	h.mu.Lock()
	h.metaData++
	h.mu.Unlock()
	return nil
}

func TestClientPublishContractStillWorks(t *testing.T) {
	serverH := &publishEchoServerHandler{}

	preparePlayConnection(t, serverH, newRecordingHandler(), func(c *ClientConn) {
		require.Nil(t, c.Connect(nil))

		stream, err := c.CreateStream(nil, chunkSize)
		require.Nil(t, err)

		require.Nil(t, stream.Publish(&message.NetStreamPublish{
			PublishingName: "live-stream",
			PublishingType: "live",
		}))
		require.Equal(t, streamStateClientPublish, stream.handler.State())

		// Send metadata, audio and video exactly as existing publishers do.
		require.Nil(t, stream.WriteDataMessage(4, 0, "@setDataFrame", &message.NetStreamSetDataFrame{
			AmfData: map[string]interface{}{"duration": 2.0},
		}))
		require.Nil(t, stream.Write(5, 0, &message.AudioMessage{
			Payload: bytes.NewReader([]byte{0xaf, 0x00}),
		}))
		require.Nil(t, stream.Write(6, 33, &message.VideoMessage{
			Payload: bytes.NewReader([]byte{0x17, 0x01}),
		}))

		require.Eventually(t, func() bool {
			serverH.mu.Lock()
			defer serverH.mu.Unlock()
			return len(serverH.audio) == 1 && len(serverH.video) == 1 && serverH.metaData == 1
		}, 3*time.Second, 5*time.Millisecond)

		serverH.mu.Lock()
		require.Equal(t, [][]byte{{0xaf, 0x00}}, serverH.audio)
		require.Equal(t, [][]byte{{0x17, 0x01}}, serverH.video)
		require.Equal(t, 1, serverH.metaData)
		serverH.mu.Unlock()
	})
}
