//
// Copyright (c) 2018- yutopp (yutopp@gmail.com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at  https://www.boost.org/LICENSE_1_0.txt)
//

package message

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// buildFlvTag builds a single FLV tag as it appears in an aggregate message body.
func buildFlvTag(tagType byte, timestamp uint32, streamID uint32, payload []byte) []byte {
	buf := make([]byte, flvTagHeaderSize+len(payload)+flvTagPrevSizeSize)
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

	copy(buf[flvTagHeaderSize:], payload)

	binary.BigEndian.PutUint32(
		buf[flvTagHeaderSize+len(payload):],
		uint32(flvTagHeaderSize)+size,
	)

	return buf
}

func amf0String(s string) []byte {
	buf := []byte{0x02, byte(len(s) >> 8), byte(len(s))}
	return append(buf, []byte(s)...)
}

func TestDecodeAggregateMessage(t *testing.T) {
	firstTimestamp := uint32(0x01000010)
	secondTimestamp := uint32(0x01000030)

	scriptPayload := append(amf0String("@setDataFrame"), 0x05) // name + AMF0 null
	body := new(bytes.Buffer)
	body.Write(buildFlvTag(flvTagTypeAudio, firstTimestamp, 0, []byte("audio-data")))
	body.Write(buildFlvTag(flvTagTypeVideo, secondTimestamp, 0, []byte("video-data")))
	body.Write(buildFlvTag(flvTagTypeScriptData, firstTimestamp, 0, scriptPayload))

	dec := NewDecoder(bytes.NewReader(body.Bytes()))

	var msg Message
	require.NoError(t, dec.Decode(TypeIDAggregateMessage, &msg))

	aggMsg, ok := msg.(*AggregateMessage)
	require.True(t, ok)
	require.Len(t, aggMsg.SubMessages, 3)

	// Sub-messages keep their own type, stream ID and payload.
	audio, ok := aggMsg.SubMessages[0].Message.(*AudioMessage)
	require.True(t, ok)
	audioPayload, err := ioutil.ReadAll(audio.Payload)
	require.NoError(t, err)
	require.Equal(t, []byte("audio-data"), audioPayload)
	require.Equal(t, firstTimestamp, aggMsg.SubMessages[0].Timestamp)

	video, ok := aggMsg.SubMessages[1].Message.(*VideoMessage)
	require.True(t, ok)
	videoPayload, err := ioutil.ReadAll(video.Payload)
	require.NoError(t, err)
	require.Equal(t, []byte("video-data"), videoPayload)
	require.Equal(t, secondTimestamp, aggMsg.SubMessages[1].Timestamp)

	data, ok := aggMsg.SubMessages[2].Message.(*DataMessage)
	require.True(t, ok)
	require.Equal(t, "@setDataFrame", data.Name)
	require.Equal(t, EncodingTypeAMF0, data.Encoding)
	dataBody, err := ioutil.ReadAll(data.Body)
	require.NoError(t, err)
	require.Equal(t, []byte{0x05}, dataBody)

	// Timestamps are rebased onto the timestamp of the aggregate message.
	require.Equal(t, uint32(5000), aggMsg.TimestampAt(5000, 0))
	require.Equal(t, uint32(5032), aggMsg.TimestampAt(5000, 1))
	require.Equal(t, uint32(5000), aggMsg.TimestampAt(5000, 2))

	// Rebasing is stable across multiple aggregates: the same deltas are
	// applied on top of each aggregate's own base timestamp.
	require.Equal(t, uint32(9000), aggMsg.TimestampAt(9000, 0))
	require.Equal(t, uint32(9032), aggMsg.TimestampAt(9000, 1))

	// Out of range indexes are clamped to the base timestamp.
	require.Equal(t, uint32(5000), aggMsg.TimestampAt(5000, 3))
	require.Equal(t, uint32(5000), aggMsg.TimestampAt(5000, -1))
}

func TestDecodeAggregateSubMessagePayloadIsBounded(t *testing.T) {
	body := new(bytes.Buffer)
	body.Write(buildFlvTag(flvTagTypeAudio, 0, 0, []byte("first")))
	body.Write(buildFlvTag(flvTagTypeVideo, 10, 0, []byte("second")))
	body.Write(buildFlvTag(flvTagTypeAudio, 20, 0, []byte("third")))

	dec := NewDecoder(bytes.NewReader(body.Bytes()))

	var msg Message
	require.NoError(t, dec.Decode(TypeIDAggregateMessage, &msg))
	aggMsg := msg.(*AggregateMessage)
	require.Len(t, aggMsg.SubMessages, 3)

	// Reading a payload must never consume the bytes of the following
	// sub-messages, and a consumer reading to the end must get exactly its own
	// bytes.
	firstPayload, err := ioutil.ReadAll(aggMsg.SubMessages[0].Message.(*AudioMessage).Payload)
	require.NoError(t, err)
	require.Equal(t, []byte("first"), firstPayload)

	secondPayload, err := ioutil.ReadAll(aggMsg.SubMessages[1].Message.(*VideoMessage).Payload)
	require.NoError(t, err)
	require.Equal(t, []byte("second"), secondPayload)

	thirdPayload, err := ioutil.ReadAll(aggMsg.SubMessages[2].Message.(*AudioMessage).Payload)
	require.NoError(t, err)
	require.Equal(t, []byte("third"), thirdPayload)

	// Reading again the same sub-message payload does not leak other data.
	rest, err := ioutil.ReadAll(aggMsg.SubMessages[1].Message.(*VideoMessage).Payload)
	require.NoError(t, err)
	require.Empty(t, rest)
}

func TestDecodeAggregateMessagePreservesSubMessageStreamID(t *testing.T) {
	body := new(bytes.Buffer)
	body.Write(buildFlvTag(flvTagTypeAudio, 0, 42, []byte("audio")))

	dec := NewDecoder(bytes.NewReader(body.Bytes()))

	var msg Message
	require.NoError(t, dec.Decode(TypeIDAggregateMessage, &msg))
	aggMsg := msg.(*AggregateMessage)
	require.Len(t, aggMsg.SubMessages, 1)
	require.Equal(t, uint32(42), aggMsg.SubMessages[0].StreamID)
}

func TestDecodeAggregateMessageEmpty(t *testing.T) {
	dec := NewDecoder(bytes.NewReader(nil))

	var msg Message
	require.NoError(t, dec.Decode(TypeIDAggregateMessage, &msg))
	aggMsg := msg.(*AggregateMessage)
	require.Empty(t, aggMsg.SubMessages)
}

func TestEncodeAggregateMessage(t *testing.T) {
	aggMsg := &AggregateMessage{
		SubMessages: []AggregateSubMessage{
			{
				Message:   &AudioMessage{Payload: bytes.NewReader([]byte("audio-data"))},
				Timestamp: 0x01000010,
			},
			{
				Message:   &VideoMessage{Payload: bytes.NewReader([]byte("video-data"))},
				Timestamp: 0x01000030,
				StreamID:  42,
			},
			{
				Message:   &DataMessage{Name: "@setDataFrame", Encoding: EncodingTypeAMF0, Body: bytes.NewReader([]byte{0x05})},
				Timestamp: 0x01000010,
			},
		},
	}

	expected := new(bytes.Buffer)
	expected.Write(buildFlvTag(flvTagTypeAudio, 0x01000010, 0, []byte("audio-data")))
	expected.Write(buildFlvTag(flvTagTypeVideo, 0x01000030, 42, []byte("video-data")))
	expected.Write(buildFlvTag(flvTagTypeScriptData, 0x01000010, 0, append(amf0String("@setDataFrame"), 0x05)))

	enc := NewEncoder(new(bytes.Buffer))
	buf := enc.w.(*bytes.Buffer)
	require.NoError(t, enc.Encode(aggMsg))
	require.Equal(t, expected.Bytes(), buf.Bytes())

	// Round trip
	dec := NewDecoder(bytes.NewReader(buf.Bytes()))
	var msg Message
	require.NoError(t, dec.Decode(TypeIDAggregateMessage, &msg))
	decoded := msg.(*AggregateMessage)
	require.Len(t, decoded.SubMessages, 3)

	audioPayload, err := ioutil.ReadAll(decoded.SubMessages[0].Message.(*AudioMessage).Payload)
	require.NoError(t, err)
	require.Equal(t, []byte("audio-data"), audioPayload)

	video := decoded.SubMessages[1]
	require.Equal(t, uint32(0x01000030), video.Timestamp)
	require.Equal(t, uint32(42), video.StreamID)
	videoPayload, err := ioutil.ReadAll(video.Message.(*VideoMessage).Payload)
	require.NoError(t, err)
	require.Equal(t, []byte("video-data"), videoPayload)

	data := decoded.SubMessages[2].Message.(*DataMessage)
	require.Equal(t, "@setDataFrame", data.Name)
	dataBody, err := ioutil.ReadAll(data.Body)
	require.NoError(t, err)
	require.Equal(t, []byte{0x05}, dataBody)
}

func TestEncodeAggregateMessageUnsupported(t *testing.T) {
	enc := NewEncoder(new(bytes.Buffer))

	err := enc.Encode(&AggregateMessage{
		SubMessages: []AggregateSubMessage{
			{
				Message: &CommandMessage{
					CommandName: "connect",
					Encoding:    EncodingTypeAMF0,
					Body:        bytes.NewReader(nil),
				},
			},
		},
	})
	require.Error(t, err)

	err = enc.Encode(&AggregateMessage{
		SubMessages: []AggregateSubMessage{
			{
				Message:  &AudioMessage{Payload: bytes.NewReader(nil)},
				StreamID: 0x1000000, // 25bits, does not fit in 24bits
			},
		},
	})
	require.Error(t, err)
}

func TestDecodeAggregateMessageCorruption(t *testing.T) {
	validSub := buildFlvTag(flvTagTypeAudio, 0, 0, []byte("audio-data"))

	tcs := []struct {
		name            string
		body            []byte
		expectedIndex   int
		expectedOffset  int
		expectedErrPart string
	}{
		{
			name:           "malformed body: less than a header",
			body:           []byte{0x08, 0x00, 0x00, 0x01, 0x00},
			expectedIndex:  0,
			expectedOffset: 0,
		},
		{
			name: "truncated payload",
			body: func() []byte {
				// Header claims 16 bytes of payload but only 3 are present.
				header := buildFlvTag(flvTagTypeAudio, 0, 0, []byte("xxxxxxxxxxxxxxxx"))
				return header[:flvTagHeaderSize+3]
			}(),
			expectedIndex:  0,
			expectedOffset: 0,
		},
		{
			name: "truncated boundary field",
			body: func() []byte {
				sub := buildFlvTag(flvTagTypeAudio, 0, 0, []byte("audio-data"))
				return sub[:len(sub)-2] // drop 2 bytes of PreviousTagSize
			}(),
			expectedIndex:  0,
			expectedOffset: 0,
		},
		{
			name: "invalid boundary field",
			body: func() []byte {
				sub := buildFlvTag(flvTagTypeAudio, 0, 0, []byte("audio-data"))
				binary.BigEndian.PutUint32(sub[len(sub)-4:], 12345)
				return sub
			}(),
			expectedIndex:   0,
			expectedOffset:  0,
			expectedErrPart: "invalid sub-message boundary field",
		},
		{
			name: "unknown sub-message type",
			body: func() []byte {
				sub := buildFlvTag(flvTagTypeAudio, 0, 0, []byte("audio-data"))
				sub[0] = 0 // not audio/video/script
				return sub
			}(),
			expectedIndex:  0,
			expectedOffset: 0,
		},
		{
			name: "corruption after a valid sub-message is located",
			body: func() []byte {
				bad := buildFlvTag(flvTagTypeVideo, 0, 0, []byte("video-data"))
				bad = bad[:len(bad)-1]
				return append(append([]byte{}, validSub...), bad...)
			}(),
			expectedIndex:  1,
			expectedOffset: len(validSub),
		},
		{
			name: "trailing garbage is not reinterpreted as a new sub-message",
			body: func() []byte {
				return append(append([]byte{}, validSub...), 0x08, 0x00, 0x00)
			}(),
			expectedIndex:  1,
			expectedOffset: len(validSub),
		},
	}

	for _, tc := range tcs {
		tc := tc // capture

		t.Run(tc.name, func(t *testing.T) {
			dec := NewDecoder(bytes.NewReader(tc.body))

			var msg Message
			err := dec.Decode(TypeIDAggregateMessage, &msg)
			require.Error(t, err)
			require.Nil(t, msg)

			var decErr *AggregateMessageDecodeError
			require.True(t, errors.As(err, &decErr), "Error = %+v", err)
			require.Equal(t, tc.expectedIndex, decErr.SubMessageIndex)
			require.Equal(t, tc.expectedOffset, decErr.Offset)
			require.NotEmpty(t, decErr.Reason)
			if tc.expectedErrPart != "" {
				require.Contains(t, err.Error(), tc.expectedErrPart)
			}
		})
	}
}

func TestDecodeAggregateMessageStopsAtFirstCorruption(t *testing.T) {
	// Two valid sub-messages followed by garbage. The decoder must report the
	// corrupted sub-message instead of silently accepting a partial result.
	body := new(bytes.Buffer)
	body.Write(buildFlvTag(flvTagTypeAudio, 0, 0, []byte("first")))
	body.Write(buildFlvTag(flvTagTypeVideo, 10, 0, []byte("second")))
	body.Write([]byte{0x12, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07})

	var msg Message
	err := NewDecoder(bytes.NewReader(body.Bytes())).Decode(TypeIDAggregateMessage, &msg)
	require.Error(t, err)
	require.Nil(t, msg)

	var decErr *AggregateMessageDecodeError
	require.True(t, errors.As(err, &decErr))
	require.Equal(t, 2, decErr.SubMessageIndex)
}

// Ensure the aggregate decoder does not consume bytes beyond the body it was
// given. This mirrors the chunk stream reader which bounds the body to the
// message length.
func TestDecodeAggregateMessageDoesNotConsumeFollowingBytes(t *testing.T) {
	body := buildFlvTag(flvTagTypeAudio, 0, 0, []byte("audio-data"))
	trailing := []byte("TRAILING-MESSAGE-BYTES")

	r := bytes.NewReader(append(append([]byte{}, body...), trailing...))
	// The chunk streamer hands a reader bounded by the message length.
	dec := NewDecoder(io.LimitReader(r, int64(len(body))))

	var msg Message
	require.NoError(t, dec.Decode(TypeIDAggregateMessage, &msg))
	require.Equal(t, []byte("audio-data"),
		mustReadAll(t, msg.(*AggregateMessage).SubMessages[0].Message.(*AudioMessage).Payload))

	// Bytes which follow the aggregate message are still available for the
	// next message.
	require.Equal(t, trailing, mustReadAll(t, r))
}

func mustReadAll(t *testing.T, r io.Reader) []byte {
	t.Helper()

	b, err := ioutil.ReadAll(r)
	require.NoError(t, err)
	return b
}

// The decoder must not retain a reference to the transient buffer which holds
// the aggregate body: the chunk stream reader reuses its buffer for the next
// message, so reading a sub-message payload later must still return the right
// bytes.
func TestDecodeAggregateMessagePayloadSurvivesBufferReuse(t *testing.T) {
	body := new(bytes.Buffer)
	body.Write(buildFlvTag(flvTagTypeAudio, 0, 0, []byte("first")))
	body.Write(buildFlvTag(flvTagTypeVideo, 10, 0, []byte("second")))

	// A mutable reader emulating the chunk stream buffer: its contents are
	// overwritten once the aggregate message has been decoded.
	reusable := append([]byte{}, body.Bytes()...)
	dec := NewDecoder(bytes.NewReader(reusable))

	var msg Message
	require.NoError(t, dec.Decode(TypeIDAggregateMessage, &msg))

	for i := range reusable {
		reusable[i] = 0xff
	}

	aggMsg := msg.(*AggregateMessage)
	require.Equal(t, []byte("first"),
		mustReadAll(t, aggMsg.SubMessages[0].Message.(*AudioMessage).Payload))
	require.Equal(t, []byte("second"),
		mustReadAll(t, aggMsg.SubMessages[1].Message.(*VideoMessage).Payload))
}
