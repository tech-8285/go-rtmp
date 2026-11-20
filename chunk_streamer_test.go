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
	"fmt"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/require"

	"github.com/yutopp/go-rtmp/message"
)

func TestStreamerSingleChunk(t *testing.T) {
	buf := new(bytes.Buffer)
	inbuf := bufio.NewReaderSize(buf, 2048)
	outbuf := bufio.NewWriterSize(buf, 2048)

	streamer := NewChunkStreamer(inbuf, outbuf, nil)

	chunkStreamID := 2
	videoContent := []byte("testtesttest")
	msg := &message.VideoMessage{
		Payload: bytes.NewReader(videoContent),
	}
	timestamp := uint32(72)

	// write a message
	w, err := streamer.NewChunkWriter(context.Background(), chunkStreamID)
	require.Nil(t, err)
	require.NotNil(t, w)

	enc := message.NewEncoder(w)
	err = enc.Encode(msg)
	require.Nil(t, err)
	w.messageLength = uint32(w.buf.Len())
	w.messageTypeID = byte(msg.TypeID())
	w.timestamp = timestamp
	err = streamer.Sched(w)
	require.Nil(t, err)

	_, err = streamer.NewChunkWriter(context.Background(), chunkStreamID) // wait for writing
	require.Nil(t, err)

	// read a message
	r, err := streamer.readChunk()
	require.Nil(t, err)
	require.NotNil(t, r)
	require.True(t, r.completed)

	dec := message.NewDecoder(r)
	var actualMsg message.Message
	err = dec.Decode(message.TypeID(r.messageTypeID), &actualMsg)
	require.Nil(t, err)
	require.Equal(t, uint32(timestamp), r.timestamp)

	// check message
	require.Equal(t, actualMsg.TypeID(), msg.TypeID())
	actualMsgT := actualMsg.(*message.VideoMessage)
	actualContent, _ := ioutil.ReadAll(actualMsgT.Payload)
	require.Equal(t, actualContent, videoContent)
}

func TestStreamerMultipleChunk(t *testing.T) {
	const chunkSize = 128
	const payloadUnit = "test"

	buf := bytes.NewBuffer(make([]byte, 0, 2048))
	inbuf := bufio.NewReaderSize(buf, 2048)
	outbuf := bufio.NewWriterSize(buf, 2048)

	streamer := NewChunkStreamer(inbuf, outbuf, nil)

	chunkStreamID := 2
	videoContent := []byte(strings.Repeat(payloadUnit, chunkSize))
	msg := &message.VideoMessage{
		// will be chunked (chunkSize * len(payloadUnit))
		Payload: bytes.NewReader(videoContent),
	}
	timestamp := uint32(72)

	// write a message
	w, err := streamer.NewChunkWriter(context.Background(), chunkStreamID)
	require.Nil(t, err)
	require.NotNil(t, w)

	enc := message.NewEncoder(w)
	err = enc.Encode(msg)
	require.Nil(t, err)
	w.messageLength = uint32(w.buf.Len())
	w.messageTypeID = byte(msg.TypeID())
	w.timestamp = timestamp
	err = streamer.Sched(w)
	require.Nil(t, err)

	_, err = streamer.NewChunkWriter(context.Background(), chunkStreamID) // wait for writing
	require.Nil(t, err)

	// read a message
	var r *ChunkStreamReader
	for i := 0; i < len(payloadUnit); i++ {
		r, err = streamer.readChunk()
		require.Nil(t, err)
	}
	require.NotNil(t, r)

	dec := message.NewDecoder(r)
	var actualMsg message.Message
	err = dec.Decode(message.TypeID(r.messageTypeID), &actualMsg)
	require.Nil(t, err)
	require.Equal(t, uint32(timestamp), r.timestamp)

	// check message
	require.Equal(t, actualMsg.TypeID(), msg.TypeID())
	actualMsgT := actualMsg.(*message.VideoMessage)
	actualContent, _ := ioutil.ReadAll(actualMsgT.Payload)
	require.Equal(t, actualContent, videoContent)
}

func TestStreamerChunkExample1(t *testing.T) {
	type write struct {
		timestamp uint32
		length    int
	}

	type read struct {
		timestamp  uint32
		fmt        byte
		isComplete bool
	}

	type testCase struct {
		name            string
		chunkStreamID   int
		typeID          byte
		messageStreamID uint32
		writeCases      []write
		readCases       []read
	}

	tcs := []testCase{
		// Example #1
		{
			name:            "Example #1",
			chunkStreamID:   3,
			typeID:          8,
			messageStreamID: 12345,
			writeCases: []write{
				{timestamp: 1000, length: 32},
				{timestamp: 1020, length: 32},
				{timestamp: 1040, length: 32},
				{timestamp: 1060, length: 32},
			},
			readCases: []read{
				{timestamp: 1000, fmt: 0, isComplete: true},
				{timestamp: 1020, fmt: 2, isComplete: true},
				{timestamp: 1040, fmt: 3, isComplete: true},
				{timestamp: 1060, fmt: 3, isComplete: true},
			},
		},
		// Example #2
		{
			name:            "Example #2",
			chunkStreamID:   4,
			typeID:          9,
			messageStreamID: 12346,
			writeCases: []write{
				{timestamp: 1000, length: 307},
			},
			readCases: []read{
				{timestamp: 1000, fmt: 0},
				{timestamp: 1000, fmt: 3},
				{timestamp: 1000, fmt: 3, isComplete: true},
			},
		},
		// Original #1 fmt0 -> fmt3, fmt2 -> fmt3
		{
			name:            "Original #1",
			chunkStreamID:   5,
			typeID:          10,
			messageStreamID: 22346,
			writeCases: []write{
				{timestamp: 1000, length: 200},
				{timestamp: 2000, length: 200},
			},
			readCases: []read{
				{timestamp: 1000, fmt: 0},
				{timestamp: 1000, fmt: 3, isComplete: true},
				{timestamp: 1000, fmt: 2}, // timestamp delta is not updated in this time
				{timestamp: 2000, fmt: 3, isComplete: true},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			buf := bytes.NewBuffer(make([]byte, 0, 2048))
			inbuf := bufio.NewReaderSize(buf, 2048)
			outbuf := bufio.NewWriterSize(buf, 2048)

			streamer := NewChunkStreamer(inbuf, outbuf, nil)

			for i, wc := range tc.writeCases {
				t.Run(fmt.Sprintf("Write: %d", i), func(t *testing.T) {
					w, err := streamer.NewChunkWriter(context.Background(), tc.chunkStreamID)
					require.Nil(t, err)
					require.NotNil(t, w)

					bin := make([]byte, wc.length)

					w.messageLength = uint32(len(bin))
					w.messageTypeID = tc.typeID
					w.messageStreamID = tc.messageStreamID
					w.timestamp = wc.timestamp
					w.buf.Write(bin)

					err = streamer.Sched(w)
					require.Nil(t, err)
				})
			}

			_, err := streamer.NewChunkWriter(context.Background(), tc.chunkStreamID) // wait for writing
			require.Nil(t, err)

			for i, rc := range tc.readCases {
				t.Run(fmt.Sprintf("Read: %d", i), func(t *testing.T) {
					r, err := streamer.readChunk()
					require.Nil(t, err)
					require.NotNil(t, r)

					require.Equal(t, rc.fmt, r.basicHeader.fmt)
					require.Equal(t, uint32(rc.timestamp), r.timestamp)
					require.Equal(t, rc.isComplete, r.completed)
				})
			}
		})
	}
}

func TestStreamerChunkExample2(t *testing.T) {
	type write struct {
		timestamp     uint32
		length        int
		messageTypeId byte
	}

	type read struct {
		timestamp  uint32
		delta      uint32
		fmt        byte
		isComplete bool
	}

	type testCase struct {
		name            string
		chunkStreamID   int
		messageStreamID uint32
		writeCases      []write
		readCases       []read
	}

	tcs := []testCase{
		// Same timestamp
		{
			name:            "Same timestamp's delta #1",
			chunkStreamID:   5,
			messageStreamID: 22346,
			writeCases: []write{
				{timestamp: 1000, length: 200, messageTypeId: 10},
				{timestamp: 1001, length: 200, messageTypeId: 11},
				{timestamp: 2000, length: 200, messageTypeId: 10},
				{timestamp: 2000, length: 200, messageTypeId: 11},
			},
			readCases: []read{
				{timestamp: 1000, delta: 0, fmt: 0, isComplete: false},
				{timestamp: 1000, delta: 0, fmt: 3, isComplete: true},

				{timestamp: 1000, delta: 1, fmt: 1, isComplete: false},
				{timestamp: 1001, delta: 0, fmt: 3, isComplete: true},

				{timestamp: 1001, delta: 999, fmt: 1, isComplete: false},
				{timestamp: 2000, delta: 0, fmt: 3, isComplete: true},

				{timestamp: 2000, delta: 0, fmt: 1, isComplete: false},
				{timestamp: 2000, delta: 0, fmt: 3, isComplete: true},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			buf := bytes.NewBuffer(make([]byte, 0, 2048))
			inbuf := bufio.NewReaderSize(buf, 2048)
			outbuf := bufio.NewWriterSize(buf, 2048)

			streamer := NewChunkStreamer(inbuf, outbuf, nil)

			for i, wc := range tc.writeCases {
				t.Run(fmt.Sprintf("Write: %d", i), func(t *testing.T) {
					w, err := streamer.NewChunkWriter(context.Background(), tc.chunkStreamID)
					require.Nil(t, err)
					require.NotNil(t, w)

					bin := make([]byte, wc.length)

					w.messageLength = uint32(len(bin))
					w.messageTypeID = byte(wc.messageTypeId)
					w.messageStreamID = tc.messageStreamID
					w.timestamp = wc.timestamp
					w.buf.Write(bin)
					err = streamer.Sched(w)
					require.Nil(t, err)
				})
			}

			_, err := streamer.NewChunkWriter(context.Background(), tc.chunkStreamID) // wait for writing
			require.Nil(t, err)

			for i, rc := range tc.readCases {
				t.Run(fmt.Sprintf("Read: %d", i), func(t *testing.T) {
					r, err := streamer.readChunk()
					_ = rc
					_ = err
					require.Nil(t, err)
					require.NotNil(t, r)
					require.Equal(t, rc.fmt, r.basicHeader.fmt)
					require.Equal(t, uint32(rc.delta), r.messageHeader.timestampDelta)
					require.Equal(t, rc.isComplete, r.completed)
				})
			}
		})
	}
}

func TestWriteToInvalidWriter(t *testing.T) {
	buf := bytes.NewBuffer(make([]byte, 0, 2048))
	inbuf := bufio.NewReaderSize(buf, 2048)

	streamer := NewChunkStreamer(inbuf, &AlwaysErrorWriter{}, nil)

	// Write some data
	chunkStreamID := 10
	timestamp := uint32(0)
	err := streamer.Write(context.Background(), chunkStreamID, timestamp, &ChunkMessage{
		StreamID: 0,
		Message:  &message.Ack{},
	})
	require.Nil(t, err)

	<-streamer.Done()
	require.EqualErrorf(t, streamer.Err(), "Always error!", "")
}

type AlwaysErrorWriter struct{}

func (w *AlwaysErrorWriter) Write(buf []byte) (int, error) {
	return 0, fmt.Errorf("Always error!")
}

func TestChunkStreamerHasNoLeaksOfGoroutines(t *testing.T) {
	defer leaktest.Check(t)()

	buf := new(bytes.Buffer)
	inbuf := bufio.NewReaderSize(buf, 2048)
	outbuf := bufio.NewWriterSize(buf, 2048)

	streamer := NewChunkStreamer(inbuf, outbuf, nil)

	err := streamer.Close()
	require.Nil(t, err)

	<-streamer.Done()
}

func TestChunkStreamerStreamsLimitation(t *testing.T) {
	buf := new(bytes.Buffer)
	inbuf := bufio.NewReaderSize(buf, 2048)
	outbuf := bufio.NewWriterSize(buf, 2048)

	streamer := NewChunkStreamer(inbuf, outbuf, &StreamControlStateConfig{
		MaxChunkStreams: 1,
	})
	defer streamer.Close()

	{
		_, err := streamer.prepareChunkReader(0)
		require.Nil(t, err)

		_, err = streamer.prepareChunkReader(1)
		require.EqualError(t, err, "Creating chunk streams limit exceeded(Reader): Limit = 1")
	}

	{
		_, err := streamer.prepareChunkWriter(0)
		require.Nil(t, err)

		_, err = streamer.prepareChunkWriter(1)
		require.EqualError(t, err, "Creating chunk streams limit exceeded(Writer): Limit = 1")
	}
}

func TestChunkStreamerDualWriter(t *testing.T) {
	buf := new(bytes.Buffer)
	inbuf := bufio.NewReaderSize(buf, 2048)
	outbuf := bufio.NewWriterSize(ioutil.Discard, 2048)

	streamer := NewChunkStreamer(inbuf, outbuf, nil)

	largePayload := []byte(strings.Repeat("abcdabcd12341234", 512))

	// Writes messages alternately to two streams 20 times without clogging
	const N = 20
	for i := 0; i < N; i++ {
		chunkStreamID := 10 + i%2

		// Write some data
		timestamp := uint32(0)
		err := streamer.Write(context.Background(), chunkStreamID, timestamp, &ChunkMessage{
			StreamID: 0,
			Message: &message.VideoMessage{
				Payload: bytes.NewReader(largePayload),
			},
		})
		require.Nil(t, err)
	}

	streamer.waitWriters()

	err := streamer.Close()
	require.Nil(t, err)

	<-streamer.Done()
	require.Equal(t, nil, streamer.Err())
}

func TestChunkStreamerDualWriterWithoutWaiting(t *testing.T) {
	buf := new(bytes.Buffer)
	inbuf := bufio.NewReaderSize(buf, 2048)
	outbuf := bufio.NewWriterSize(ioutil.Discard, 2048)

	streamer := NewChunkStreamer(inbuf, outbuf, nil)

	largePayload := []byte(strings.Repeat("abcdabcd12341234", 512))

	// Writes messages alternately to two streams 20 times without clogging
	const N = 20
	for i := 0; i < N; i++ {
		chunkStreamID := 10 + i%2

		// Write some data
		timestamp := uint32(0)
		err := streamer.Write(context.Background(), chunkStreamID, timestamp, &ChunkMessage{
			StreamID: 0,
			Message: &message.VideoMessage{
				Payload: bytes.NewReader(largePayload),
			},
		})
		require.Nil(t, err)
	}

	err := streamer.Close()
	require.Nil(t, err)

	<-streamer.Done()
	require.Equal(t, nil, streamer.Err())
}

func TestChunkStreamerNewChunkWriterTwice(t *testing.T) {
	buf := new(bytes.Buffer)
	inbuf := bufio.NewReaderSize(buf, 2048)
	outbuf := bufio.NewWriterSize(ioutil.Discard, 2048)

	streamer := NewChunkStreamer(inbuf, outbuf, nil)

	chunkStreamID := 10

	_, err := streamer.NewChunkWriter(context.Background(), chunkStreamID)
	require.Nil(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = streamer.NewChunkWriter(ctx, chunkStreamID) // Try to acquire same chunk writer
	require.EqualError(t, err, "Failed to wait chunk writer: context deadline exceeded")

	err = streamer.Close()
	require.Nil(t, err)

	<-streamer.Done()
	require.Equal(t, nil, streamer.Err())
}

func BenchmarkStreamerMultipleChunkRead(b *testing.B) {
	const chunkSize = 128
	const payloadUnit = "test"

	buf := bytes.NewBuffer(nil)
	streamer := NewChunkStreamer(buf, buf, nil)

	chunkStreamID := 2
	videoContent := []byte(strings.Repeat(payloadUnit, chunkSize))
	msg := &message.VideoMessage{
		// will be chunked (chunkSize * len(payloadUnit))
		Payload: bytes.NewReader(videoContent),
	}
	timestamp := uint32(72)

	// write a message
	w, _ := streamer.NewChunkWriter(context.Background(), chunkStreamID)
	enc := message.NewEncoder(w)
	_ = enc.Encode(msg)

	w.messageLength = uint32(w.buf.Len())
	w.messageTypeID = byte(msg.TypeID())
	w.timestamp = timestamp
	_ = streamer.Sched(w)

	_, _ = streamer.NewChunkWriter(context.Background(), chunkStreamID) // wait for writing

	r := bytes.NewReader(buf.Bytes())
	s := NewChunkStreamer(r, nil, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Reset(buf.Bytes())

		_, err := s.NewChunkReader()
		if err != nil {
			b.Error(err)
		}
	}
}

// Wire-level sequence which alternates extended timestamps and fmt=3 headers.
// fmt=3 chunks repeat the extended timestamp field of the previous chunk on
// the same chunk stream (RTMP spec 5.3.1.3).
//
// chunk stream 6:
//
//	msg1: ts=0x01000000, len=200 (fragmented: 128 + 72)
//	msg2: ts=0x03000000 (delta=0x02000000), len=4
//	msg3: ts=0x05000000 (delta=0x02000000), len=4
func extendedTimestampWireBytes() []byte {
	var b []byte

	payload1 := append(bytes.Repeat([]byte{0xaa}, 128), bytes.Repeat([]byte{0xbb}, 72)...)

	// msg1, chunk1: fmt=0, csid=6, ts=0xffffff(marker), len=200, type=9, msid=1, ext=0x01000000
	b = append(b, 0x06)
	b = append(b, 0xff, 0xff, 0xff)       // timestamp marker
	b = append(b, 0x00, 0x00, 0xc8)       // message length = 200
	b = append(b, 0x09)                   // message type id
	b = append(b, 0x01, 0x00, 0x00, 0x00) // message stream id (LE)
	b = append(b, 0x01, 0x00, 0x00, 0x00) // extended timestamp
	b = append(b, payload1[:128]...)      // payload (fragment 1)

	// msg1, chunk2: fmt=3, csid=6, ext=0x01000000 (repeated)
	b = append(b, 0xc6)
	b = append(b, 0x01, 0x00, 0x00, 0x00) // extended timestamp
	b = append(b, payload1[128:]...)      // payload (fragment 2)

	// msg2: fmt=1, csid=6, delta=0xffffff(marker), len=4, type=9, ext=0x02000000
	b = append(b, 0x46)
	b = append(b, 0xff, 0xff, 0xff)       // timestamp delta marker
	b = append(b, 0x00, 0x00, 0x04)       // message length = 4
	b = append(b, 0x09)                   // message type id
	b = append(b, 0x02, 0x00, 0x00, 0x00) // extended timestamp delta
	b = append(b, 0xcc, 0xcc, 0xcc, 0xcc) // payload

	// msg3: fmt=3, csid=6, ext=0x02000000 (repeated)
	b = append(b, 0xc6)
	b = append(b, 0x02, 0x00, 0x00, 0x00) // extended timestamp delta
	b = append(b, 0xdd, 0xdd, 0xdd, 0xdd) // payload

	return b
}

func TestStreamerReadExtendedTimestampWithFmt3(t *testing.T) {
	wire := extendedTimestampWireBytes()
	inbuf := bufio.NewReader(bytes.NewReader(wire))
	outbuf := bufio.NewWriterSize(ioutil.Discard, 2048)

	streamer := NewChunkStreamer(inbuf, outbuf, nil)
	defer streamer.Close()

	// msg1, fragment 1
	r, err := streamer.readChunk()
	require.Nil(t, err)
	require.False(t, r.completed)
	require.Equal(t, uint32(0x01000000), r.timestamp)

	// msg1, fragment 2 (fmt=3 with extended timestamp)
	r, err = streamer.readChunk()
	require.Nil(t, err)
	require.True(t, r.completed)
	require.Equal(t, uint32(0x01000000), r.timestamp)
	require.Equal(t, 200, r.buf.Len())
	require.Equal(t, bytes.Repeat([]byte{0xaa}, 128), r.buf.Bytes()[:128])
	require.Equal(t, bytes.Repeat([]byte{0xbb}, 72), r.buf.Bytes()[128:])

	// msg2 (fmt=2 with extended timestamp delta)
	r, err = streamer.readChunk()
	require.Nil(t, err)
	require.True(t, r.completed)
	require.Equal(t, uint32(0x03000000), r.timestamp)
	require.Equal(t, uint32(0x02000000), r.timestampDelta)
	require.Equal(t, []byte{0xcc, 0xcc, 0xcc, 0xcc}, r.buf.Bytes())

	// msg3 (fmt=3 with extended timestamp delta, delta is inherited)
	r, err = streamer.readChunk()
	require.Nil(t, err)
	require.True(t, r.completed)
	require.Equal(t, uint32(0x05000000), r.timestamp)
	require.Equal(t, uint32(0x02000000), r.timestampDelta)
	require.Equal(t, []byte{0xdd, 0xdd, 0xdd, 0xdd}, r.buf.Bytes())
}

func TestStreamerWriteExtendedTimestampWithFmt3(t *testing.T) {
	buf := bytes.NewBuffer(make([]byte, 0, 2048))
	inbuf := bufio.NewReaderSize(buf, 2048)
	outbuf := bufio.NewWriterSize(buf, 2048)

	streamer := NewChunkStreamer(inbuf, outbuf, nil)
	defer streamer.Close()

	chunkStreamID := 6
	payload1 := append(bytes.Repeat([]byte{0xaa}, 128), bytes.Repeat([]byte{0xbb}, 72)...)

	writeCases := []struct {
		timestamp uint32
		payload   []byte
	}{
		{timestamp: 0x01000000, payload: payload1},
		{timestamp: 0x03000000, payload: []byte{0xcc, 0xcc, 0xcc, 0xcc}},
		{timestamp: 0x05000000, payload: []byte{0xdd, 0xdd, 0xdd, 0xdd}},
	}

	for _, wc := range writeCases {
		w, err := streamer.NewChunkWriter(context.Background(), chunkStreamID)
		require.Nil(t, err)

		w.messageLength = uint32(len(wc.payload))
		w.messageTypeID = 9
		w.messageStreamID = 1
		w.timestamp = wc.timestamp
		w.buf.Write(wc.payload)

		err = streamer.Sched(w)
		require.Nil(t, err)
	}

	_, err := streamer.NewChunkWriter(context.Background(), chunkStreamID) // wait for writing
	require.Nil(t, err)

	// The writer must emit the same wire format as a spec-compliant peer:
	// fmt=3 chunks repeat the extended timestamp field.
	expected := extendedTimestampWireBytes()
	require.Equal(t, expected, buf.Bytes())
}

func TestStreamerReadInterleavedChunkStreamsExtendedTimestamp(t *testing.T) {
	var wire []byte

	// csid 6, msg1: fmt=0, ts=0x01000000(ext), len=200 -> fragmented
	wire = append(wire, 0x06)
	wire = append(wire, 0xff, 0xff, 0xff)
	wire = append(wire, 0x00, 0x00, 0xc8)
	wire = append(wire, 0x09)
	wire = append(wire, 0x01, 0x00, 0x00, 0x00)
	wire = append(wire, 0x01, 0x00, 0x00, 0x00) // ext
	wire = append(wire, bytes.Repeat([]byte{0xaa}, 128)...)

	// csid 7, msg1: fmt=0, ts=100, len=4 (no ext)
	wire = append(wire, 0x07)
	wire = append(wire, 0x00, 0x00, 0x64)
	wire = append(wire, 0x00, 0x00, 0x04)
	wire = append(wire, 0x09)
	wire = append(wire, 0x01, 0x00, 0x00, 0x00)
	wire = append(wire, 0x71, 0x71, 0x71, 0x71)

	// csid 6, msg1 fragment 2: fmt=3, ext repeated
	wire = append(wire, 0xc6)
	wire = append(wire, 0x01, 0x00, 0x00, 0x00) // ext
	wire = append(wire, bytes.Repeat([]byte{0xbb}, 72)...)

	// csid 7, msg2: fmt=2, delta=40 (no ext)
	wire = append(wire, 0x87)
	wire = append(wire, 0x00, 0x00, 0x28)
	wire = append(wire, 0x72, 0x72, 0x72, 0x72)

	// csid 6, msg2: fmt=1, delta=0x01000000(ext), len=4
	wire = append(wire, 0x46)
	wire = append(wire, 0xff, 0xff, 0xff)
	wire = append(wire, 0x00, 0x00, 0x04)
	wire = append(wire, 0x09)
	wire = append(wire, 0x01, 0x00, 0x00, 0x00) // ext
	wire = append(wire, 0xcc, 0xcc, 0xcc, 0xcc)

	// csid 7, msg3: fmt=3, delta inherited (no ext)
	wire = append(wire, 0xc7)
	wire = append(wire, 0x73, 0x73, 0x73, 0x73)

	// csid 6, msg3: fmt=3, ext repeated
	wire = append(wire, 0xc6)
	wire = append(wire, 0x01, 0x00, 0x00, 0x00) // ext
	wire = append(wire, 0xdd, 0xdd, 0xdd, 0xdd)

	inbuf := bufio.NewReader(bytes.NewReader(wire))
	outbuf := bufio.NewWriterSize(ioutil.Discard, 2048)

	streamer := NewChunkStreamer(inbuf, outbuf, nil)
	defer streamer.Close()

	type expectation struct {
		chunkStreamID int
		timestamp     uint32
		completed     bool
	}
	expectations := []expectation{
		{chunkStreamID: 6, timestamp: 0x01000000, completed: false},
		{chunkStreamID: 7, timestamp: 100, completed: true},
		{chunkStreamID: 6, timestamp: 0x01000000, completed: true},
		{chunkStreamID: 7, timestamp: 140, completed: true},
		{chunkStreamID: 6, timestamp: 0x02000000, completed: true},
		{chunkStreamID: 7, timestamp: 180, completed: true},
		{chunkStreamID: 6, timestamp: 0x03000000, completed: true},
	}

	for i, exp := range expectations {
		r, err := streamer.readChunk()
		require.Nil(t, err, "case %d", i)
		require.Equal(t, exp.chunkStreamID, r.basicHeader.chunkStreamID, "case %d", i)
		require.Equal(t, exp.timestamp, r.timestamp, "case %d", i)
		require.Equal(t, exp.completed, r.completed, "case %d", i)
	}
}

func TestStreamerReadInvalidExtendedTimestamp(t *testing.T) {
	t.Run("fmt 0", func(t *testing.T) {
		var wire []byte
		wire = append(wire, 0x06)
		wire = append(wire, 0xff, 0xff, 0xff)       // timestamp marker
		wire = append(wire, 0x00, 0x00, 0x04)       // message length = 4
		wire = append(wire, 0x09)                   // message type id
		wire = append(wire, 0x01, 0x00, 0x00, 0x00) // message stream id (LE)
		wire = append(wire, 0x00, 0x00, 0x00, 0x10) // invalid extended timestamp (< marker)

		streamer := NewChunkStreamer(bufio.NewReader(bytes.NewReader(wire)), nil, nil)
		defer streamer.Close()

		_, err := streamer.readChunk()
		require.EqualError(t, err, "Invalid extended timestamp: 16")
	})

	t.Run("fmt 1", func(t *testing.T) {
		var wire []byte
		wire = append(wire, 0x46)
		wire = append(wire, 0xff, 0xff, 0xff)       // timestamp delta marker
		wire = append(wire, 0x00, 0x00, 0x04)       // message length = 4
		wire = append(wire, 0x09)                   // message type id
		wire = append(wire, 0x00, 0x00, 0x00, 0x10) // invalid extended timestamp delta (< marker)

		streamer := NewChunkStreamer(bufio.NewReader(bytes.NewReader(wire)), nil, nil)
		defer streamer.Close()

		_, err := streamer.readChunk()
		require.EqualError(t, err, "Invalid extended timestamp delta: 16")
	})

	t.Run("fmt 2", func(t *testing.T) {
		var wire []byte
		wire = append(wire, 0x86)
		wire = append(wire, 0xff, 0xff, 0xff)       // timestamp delta marker
		wire = append(wire, 0x00, 0x00, 0x00, 0x10) // invalid extended timestamp delta (< marker)

		streamer := NewChunkStreamer(bufio.NewReader(bytes.NewReader(wire)), nil, nil)
		defer streamer.Close()

		_, err := streamer.readChunk()
		require.EqualError(t, err, "Invalid extended timestamp delta: 16")
	})
}

func TestStreamerTimestampWrapAround(t *testing.T) {
	buf := bytes.NewBuffer(make([]byte, 0, 2048))
	inbuf := bufio.NewReaderSize(buf, 2048)
	outbuf := bufio.NewWriterSize(buf, 2048)

	streamer := NewChunkStreamer(inbuf, outbuf, nil)
	defer streamer.Close()

	chunkStreamID := 6
	writeCases := []struct {
		timestamp uint32
		fmt       byte
	}{
		// huge delta requires the extended timestamp field
		{timestamp: 0xfffffff0, fmt: 0},
		// wrapped timestamp goes back: resets to fmt=0 with an absolute timestamp
		{timestamp: 0x00000010, fmt: 0},
		// continue normally from the new absolute timestamp
		{timestamp: 0x00000030, fmt: 2},
		{timestamp: 0x00000050, fmt: 3},
	}

	for _, wc := range writeCases {
		w, err := streamer.NewChunkWriter(context.Background(), chunkStreamID)
		require.Nil(t, err)

		w.messageLength = 4
		w.messageTypeID = 9
		w.messageStreamID = 1
		w.timestamp = wc.timestamp
		w.buf.Write([]byte{0x01, 0x02, 0x03, 0x04})

		err = streamer.Sched(w)
		require.Nil(t, err)
	}

	_, err := streamer.NewChunkWriter(context.Background(), chunkStreamID) // wait for writing
	require.Nil(t, err)

	for i, wc := range writeCases {
		r, err := streamer.readChunk()
		require.Nil(t, err, "case %d", i)
		require.True(t, r.completed, "case %d", i)
		require.Equal(t, wc.fmt, r.basicHeader.fmt, "case %d", i)
		require.Equal(t, wc.timestamp, r.timestamp, "case %d", i)
	}
}
