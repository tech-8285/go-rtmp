//
// Copyright (c) 2018- yutopp (yutopp@gmail.com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at  https://www.boost.org/LICENSE_1_0.txt)
//

package rtmp

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/yutopp/go-rtmp/message"
)

// aggregateEvent is a callback recorded by aggregateRecordingHandler.
type aggregateEvent struct {
	kind      string
	timestamp uint32
	payload   []byte
	dataFrame *message.NetStreamSetDataFrame
}

type aggregateRecordingHandler struct {
	DefaultHandler

	events []aggregateEvent
}

func (h *aggregateRecordingHandler) OnAudio(timestamp uint32, payload io.Reader) error {
	b, err := ioutil.ReadAll(payload)
	if err != nil {
		return err
	}

	h.events = append(h.events, aggregateEvent{kind: "audio", timestamp: timestamp, payload: b})
	return nil
}

func (h *aggregateRecordingHandler) OnVideo(timestamp uint32, payload io.Reader) error {
	b, err := ioutil.ReadAll(payload)
	if err != nil {
		return err
	}

	h.events = append(h.events, aggregateEvent{kind: "video", timestamp: timestamp, payload: b})
	return nil
}

func (h *aggregateRecordingHandler) OnSetDataFrame(timestamp uint32, data *message.NetStreamSetDataFrame) error {
	h.events = append(h.events, aggregateEvent{kind: "data", timestamp: timestamp, dataFrame: data})
	return nil
}

func (h *aggregateRecordingHandler) OnUnknownMessage(timestamp uint32, msg message.Message) error {
	h.events = append(h.events, aggregateEvent{kind: "unknown"})
	return nil
}

// flvTagForTest builds a single FLV tag used in the body of an aggregate message.
func flvTagForTest(tagType byte, timestamp uint32, streamID uint32, payload []byte) []byte {
	buf := make([]byte, 11+len(payload)+4)
	buf[0] = tagType

	size := uint32(len(payload))
	buf[1] = byte(size >> 16)
	buf[2] = byte(size >> 8)
	buf[3] = byte(size)

	buf[4] = byte(timestamp >> 16)
	buf[5] = byte(timestamp >> 8)
	buf[6] = byte(timestamp)
	buf[7] = byte(timestamp >> 24)

	buf[8] = byte(streamID >> 16)
	buf[9] = byte(streamID >> 8)
	buf[10] = byte(streamID)

	copy(buf[11:], payload)

	binary.BigEndian.PutUint32(buf[11+len(payload):], 11+size)

	return buf
}

func amf0StringForTest(s string) []byte {
	buf := []byte{0x02, byte(len(s) >> 8), byte(len(s))}
	return append(buf, []byte(s)...)
}

// chunkMessageForTest serializes a single message into RTMP chunks (fmt 0
// followed by fmt 3 continuations) so it can be fed to the reader directly.
func chunkMessageForTest(
	chunkStreamID int,
	timestamp uint32,
	messageTypeID byte,
	messageStreamID uint32,
	payload []byte,
	chunkSize int,
) []byte {
	buf := new(bytes.Buffer)

	buf.WriteByte(byte(chunkStreamID & 0x3f)) // fmt = 0

	header := make([]byte, 11)
	header[0] = byte(timestamp >> 16)
	header[1] = byte(timestamp >> 8)
	header[2] = byte(timestamp)

	length := uint32(len(payload))
	header[3] = byte(length >> 16)
	header[4] = byte(length >> 8)
	header[5] = byte(length)

	header[6] = messageTypeID
	binary.LittleEndian.PutUint32(header[7:], messageStreamID)

	buf.Write(header)

	offset := 0
	for {
		remaining := len(payload) - offset
		if remaining > chunkSize {
			remaining = chunkSize
		}
		buf.Write(payload[offset : offset+remaining])
		offset += remaining

		if offset >= len(payload) {
			break
		}
		buf.WriteByte(0xc0 | byte(chunkStreamID&0x3f)) // fmt = 3
	}

	return buf.Bytes()
}

func newAggregateTestConn(t *testing.T, r io.Reader, handler Handler) (*Conn, *Stream) {
	t.Helper()

	c := newConn(&rwcMock{}, &ConnConfig{
		Handler:                   handler,
		SkipHandshakeVerification: true,
	})
	// Replace the reader used by the streamer with the pre-serialized stream.
	c.streamer.r.reader = r

	s, err := c.streams.Create(42)
	require.NoError(t, err)
	s.handler.ChangeState(streamStateServerPublish)

	return c, s
}

func TestHandleAggregateMessageDeliversSubMessages(t *testing.T) {
	const (
		chunkStreamID = 3
		baseTimestamp = uint32(5000)
	)

	scriptPayload := append(amf0StringForTest("@setDataFrame"), 0x05) // name + AMF0 null
	body := new(bytes.Buffer)
	body.Write(flvTagForTest(8, 0x01000010, 0, []byte("audio-1-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")))
	body.Write(flvTagForTest(9, 0x01000030, 0, []byte("video-1-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")))
	body.Write(flvTagForTest(18, 0x01000010, 0, scriptPayload))
	body.Write(flvTagForTest(8, 0x01000040, 0, []byte("audio-2-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")))

	// The body is larger than the default chunk size (128), so it spans several
	// chunks and must be reassembled before splitting.
	require.Greater(t, body.Len(), 128)

	stream := new(bytes.Buffer)
	stream.Write(chunkMessageForTest(
		chunkStreamID, baseTimestamp, byte(message.TypeIDAggregateMessage), 42, body.Bytes(), 128,
	))

	handler := &aggregateRecordingHandler{}
	c, _ := newAggregateTestConn(t, stream, handler)
	defer c.Close()

	err := c.handleMessageLoop()
	require.Equal(t, io.EOF, err) // reading past the last message

	require.Len(t, handler.events, 4)

	// Order is preserved and payloads are exactly the sub-message payloads.
	require.Equal(t, "audio", handler.events[0].kind)
	require.Equal(t, []byte("audio-1-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), handler.events[0].payload)
	require.Equal(t, uint32(5000), handler.events[0].timestamp)

	require.Equal(t, "video", handler.events[1].kind)
	require.Equal(t, []byte("video-1-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), handler.events[1].payload)
	require.Equal(t, uint32(5032), handler.events[1].timestamp)

	require.Equal(t, "data", handler.events[2].kind)
	require.Equal(t, uint32(5000), handler.events[2].timestamp)
	require.NotNil(t, handler.events[2].dataFrame)

	require.Equal(t, "audio", handler.events[3].kind)
	require.Equal(t, []byte("audio-2-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), handler.events[3].payload)
	require.Equal(t, uint32(5048), handler.events[3].timestamp)
}

func TestHandleAggregateMessageTimelineDoesNotDrift(t *testing.T) {
	buildAggregate := func(firstSubTimestamp uint32) []byte {
		body := new(bytes.Buffer)
		body.Write(flvTagForTest(8, firstSubTimestamp, 0, []byte("audio")))
		body.Write(flvTagForTest(9, firstSubTimestamp+20, 0, []byte("video")))
		return body.Bytes()
	}

	stream := new(bytes.Buffer)
	// The second aggregate carries timestamps which keep accumulating from the
	// first one, as senders typically do.
	stream.Write(chunkMessageForTest(3, 1000, byte(message.TypeIDAggregateMessage), 42, buildAggregate(0), 128))
	stream.Write(chunkMessageForTest(3, 2000, byte(message.TypeIDAggregateMessage), 42, buildAggregate(1000), 128))

	handler := &aggregateRecordingHandler{}
	c, _ := newAggregateTestConn(t, stream, handler)
	defer c.Close()

	err := c.handleMessageLoop()
	require.Equal(t, io.EOF, err)

	require.Len(t, handler.events, 4)
	require.Equal(t, uint32(1000), handler.events[0].timestamp)
	require.Equal(t, uint32(1020), handler.events[1].timestamp)
	require.Equal(t, uint32(2000), handler.events[2].timestamp)
	require.Equal(t, uint32(2020), handler.events[3].timestamp)
}

func TestHandleAggregateMessageKeepsNormalMessagesOnOriginalPath(t *testing.T) {
	body := new(bytes.Buffer)
	body.Write(flvTagForTest(8, 0, 0, []byte("audio")))

	stream := new(bytes.Buffer)
	// A normal video message before the aggregate.
	stream.Write(chunkMessageForTest(3, 100, byte(message.TypeIDVideoMessage), 42, []byte("normal-video"), 128))
	// The aggregate message.
	stream.Write(chunkMessageForTest(3, 5000, byte(message.TypeIDAggregateMessage), 42, body.Bytes(), 128))
	// A normal audio message after the aggregate.
	stream.Write(chunkMessageForTest(3, 6000, byte(message.TypeIDAudioMessage), 42, []byte("normal-audio"), 128))

	handler := &aggregateRecordingHandler{}
	c, _ := newAggregateTestConn(t, stream, handler)
	defer c.Close()

	err := c.handleMessageLoop()
	require.Equal(t, io.EOF, err)

	require.Len(t, handler.events, 3)
	require.Equal(t, "video", handler.events[0].kind)
	require.Equal(t, []byte("normal-video"), handler.events[0].payload)
	require.Equal(t, uint32(100), handler.events[0].timestamp)

	require.Equal(t, "audio", handler.events[1].kind)
	require.Equal(t, []byte("audio"), handler.events[1].payload)
	require.Equal(t, uint32(5000), handler.events[1].timestamp)

	require.Equal(t, "audio", handler.events[2].kind)
	require.Equal(t, []byte("normal-audio"), handler.events[2].payload)
	require.Equal(t, uint32(6000), handler.events[2].timestamp)
}

func TestHandleAggregateMessageCorruptionStopsDelivery(t *testing.T) {
	validAggregateBody := new(bytes.Buffer)
	validAggregateBody.Write(flvTagForTest(8, 0, 0, []byte("audio")))

	corruptAggregateBody := flvTagForTest(9, 0, 0, []byte("video-data"))
	corruptAggregateBody = corruptAggregateBody[:len(corruptAggregateBody)-1] // truncated boundary field

	stream := new(bytes.Buffer)
	// Messages delivered before the corruption are not revoked.
	stream.Write(chunkMessageForTest(3, 100, byte(message.TypeIDVideoMessage), 42, []byte("delivered"), 128))
	stream.Write(chunkMessageForTest(3, 5000, byte(message.TypeIDAggregateMessage), 42, validAggregateBody.Bytes(), 128))
	stream.Write(chunkMessageForTest(3, 6000, byte(message.TypeIDAggregateMessage), 42, corruptAggregateBody, 128))

	handler := &aggregateRecordingHandler{}
	c, _ := newAggregateTestConn(t, stream, handler)
	defer c.Close()

	err := c.handleMessageLoop()
	require.Error(t, err)

	var decErr *message.AggregateMessageDecodeError
	require.True(t, errors.As(err, &decErr), "Error = %+v", err)
	require.Equal(t, 0, decErr.SubMessageIndex)
	require.Equal(t, 0, decErr.Offset)
	require.NotEmpty(t, decErr.Reason)

	// Messages delivered before the corrupted aggregate are not revoked, and no
	// sub-message of the corrupted aggregate is handed to the application.
	require.Len(t, handler.events, 2)
	require.Equal(t, "video", handler.events[0].kind)
	require.Equal(t, []byte("delivered"), handler.events[0].payload)
	require.Equal(t, "audio", handler.events[1].kind)
	require.Equal(t, []byte("audio"), handler.events[1].payload)
}

func TestHandleAggregateMessageSubMessageStreamIDFallback(t *testing.T) {
	body := new(bytes.Buffer)
	body.Write(flvTagForTest(8, 0, 0, []byte("audio")))

	stream := new(bytes.Buffer)
	stream.Write(chunkMessageForTest(3, 5000, byte(message.TypeIDAggregateMessage), 42, body.Bytes(), 128))

	handler := &aggregateRecordingHandler{}
	c, _ := newAggregateTestConn(t, stream, handler)
	defer c.Close()

	require.Equal(t, io.EOF, c.handleMessageLoop())
	require.Len(t, handler.events, 1)
}

type loopbackEvent struct {
	kind    string
	payload []byte
}

type aggregateLoopbackHandler struct {
	DefaultHandler

	published chan struct{}
	events    chan loopbackEvent
}

func (h *aggregateLoopbackHandler) OnPublish(
	_ *StreamContext,
	_ uint32,
	_ *message.NetStreamPublish,
) error {
	select {
	case h.published <- struct{}{}:
	default:
	}
	return nil
}

func (h *aggregateLoopbackHandler) OnAudio(timestamp uint32, payload io.Reader) error {
	b, err := ioutil.ReadAll(payload)
	if err != nil {
		return err
	}

	h.events <- loopbackEvent{kind: "audio", payload: b}
	return nil
}

func (h *aggregateLoopbackHandler) OnVideo(timestamp uint32, payload io.Reader) error {
	b, err := ioutil.ReadAll(payload)
	if err != nil {
		return err
	}

	h.events <- loopbackEvent{kind: "video", payload: b}
	return nil
}

func TestHandleAggregateMessageLoopback(t *testing.T) {
	published := make(chan struct{}, 1)
	events := make(chan loopbackEvent, 8)

	serverHandler := &aggregateLoopbackHandler{
		published: published,
		events:    events,
	}

	config := &ConnConfig{
		Handler: serverHandler,
		ControlState: StreamControlStateConfig{
			MaxMessageStreams: 2,
		},
	}

	prepareConnection(t, config, func(c *ClientConn) {
		require.NoError(t, c.Connect(nil))

		s, err := c.CreateStream(nil, 128)
		require.NoError(t, err)
		defer s.Close()

		require.NoError(t, s.Publish(&message.NetStreamPublish{PublishingName: "test"}))

		select {
		case <-published:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for publish")
		}

		require.NoError(t, s.Write(3, 1000, &message.AggregateMessage{
			SubMessages: []message.AggregateSubMessage{
				{
					Message:   &message.AudioMessage{Payload: bytes.NewReader([]byte("audio-from-aggregate"))},
					Timestamp: 0,
				},
				{
					Message:   &message.VideoMessage{Payload: bytes.NewReader([]byte("video-from-aggregate"))},
					Timestamp: 40,
				},
			},
		}))

		expected := []loopbackEvent{
			{kind: "audio", payload: []byte("audio-from-aggregate")},
			{kind: "video", payload: []byte("video-from-aggregate")},
		}
		for _, exp := range expected {
			select {
			case got := <-events:
				require.Equal(t, exp.kind, got.kind)
				require.Equal(t, exp.payload, got.payload)
			case <-time.After(3 * time.Second):
				t.Fatalf("timed out waiting for %s event", exp.kind)
			}
		}
	})
}
