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
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/yutopp/go-rtmp/message"
)

// TestStreamSetChunkSize checks that the chunk size of the sending side can be
// changed in the middle of a session, and that a peer can keep parsing
// subsequent audio, video and data messages with the new size, preserving
// payloads and timestamps.
func TestStreamSetChunkSize(t *testing.T) {
	rwc := &rwcMock{}
	conn := newConn(rwc, nil)
	defer closeStreamer(t, conn)
	stream := newStream(1, conn)

	videoContent1 := bytes.Repeat([]byte("v"), 300)
	videoContent2 := bytes.Repeat([]byte("w"), 300)
	audioContent := bytes.Repeat([]byte("a"), 200)

	dataBodyBuf := new(bytes.Buffer)
	amfEnc := message.NewAMFEncoder(dataBodyBuf, message.EncodingTypeAMF0)
	require.Nil(t, message.EncodeBodyAnyValues(amfEnc, &message.NetConnectionCreateStream{}))
	dataContent := dataBodyBuf.Bytes()

	// A message before the change: chunked with the default size (128)
	writeStreamMessageAndWait(t, stream, 8, 100, &message.VideoMessage{
		Payload: bytes.NewReader(videoContent1),
	})

	// Change the chunk size in the middle of the session.
	// It blocks until the new size takes effect.
	err := stream.SetChunkSize(512)
	require.Nil(t, err)
	require.Equal(t, uint32(512), conn.streamer.SelfState().ChunkSize())

	// Messages after the change: chunked with the new size
	err = stream.Write(8, 200, &message.VideoMessage{
		Payload: bytes.NewReader(videoContent2),
	})
	require.Nil(t, err)
	err = stream.Write(9, 300, &message.AudioMessage{
		Payload: bytes.NewReader(audioContent),
	})
	require.Nil(t, err)
	// The last write is awaited: since writes are processed in order,
	// all the messages above have also been written out at this point.
	writeStreamMessageAndWait(t, stream, 10, 400, &message.DataMessage{
		Name:     "onMetaData",
		Encoding: message.EncodingTypeAMF0,
		Body:     bytes.NewReader(dataContent),
	})

	//
	// Read the written bytes as a peer would do
	//
	readerStreamer := NewChunkStreamer(bytes.NewReader(rwc.Bytes()), ioutil.Discard, nil)
	defer readerStreamer.Close()

	decodeMessage := func(r *ChunkStreamReader) message.Message {
		dec := message.NewDecoder(r)
		var msg message.Message
		require.Nil(t, dec.Decode(message.TypeID(r.messageTypeID), &msg))
		return msg
	}

	// Message 1: fragmented with the old chunk size (128)
	r, err := readerStreamer.readChunk()
	require.Nil(t, err)
	require.False(t, r.completed)
	r, err = readerStreamer.readChunk()
	require.Nil(t, err)
	require.False(t, r.completed)
	r, err = readerStreamer.readChunk()
	require.Nil(t, err)
	require.True(t, r.completed)
	require.Equal(t, uint32(100), r.timestamp)
	msg1 := decodeMessage(r).(*message.VideoMessage)
	actualContent, err := ioutil.ReadAll(msg1.Payload)
	require.Nil(t, err)
	require.Equal(t, videoContent1, actualContent)

	// SetChunkSize message: notified before messages written with the new size
	r, err = readerStreamer.readChunk()
	require.Nil(t, err)
	require.True(t, r.completed)
	require.Equal(t, message.TypeIDSetChunkSize, message.TypeID(r.messageTypeID))
	scs := decodeMessage(r).(*message.SetChunkSize)
	require.Equal(t, uint32(512), scs.ChunkSize)

	// The peer applies the notified size to subsequent chunks
	require.Nil(t, readerStreamer.PeerState().SetChunkSize(scs.ChunkSize))

	// Message 2: 300 bytes fit in a single chunk with the new size (512)
	r, err = readerStreamer.readChunk()
	require.Nil(t, err)
	require.True(t, r.completed)
	require.Equal(t, uint32(200), r.timestamp)
	msg2 := decodeMessage(r).(*message.VideoMessage)
	actualContent, err = ioutil.ReadAll(msg2.Payload)
	require.Nil(t, err)
	require.Equal(t, videoContent2, actualContent)

	// Audio message
	r, err = readerStreamer.readChunk()
	require.Nil(t, err)
	require.True(t, r.completed)
	require.Equal(t, uint32(300), r.timestamp)
	msg3 := decodeMessage(r).(*message.AudioMessage)
	actualContent, err = ioutil.ReadAll(msg3.Payload)
	require.Nil(t, err)
	require.Equal(t, audioContent, actualContent)

	// Data message
	r, err = readerStreamer.readChunk()
	require.Nil(t, err)
	require.True(t, r.completed)
	require.Equal(t, uint32(400), r.timestamp)
	msg4 := decodeMessage(r).(*message.DataMessage)
	require.Equal(t, "onMetaData", msg4.Name)
	actualContent, err = ioutil.ReadAll(msg4.Body)
	require.Nil(t, err)
	require.Equal(t, dataContent, actualContent)

	// No more bytes are written
	require.Equal(t, uint64(len(rwc.Bytes())), uint64(readerStreamer.r.TotalReadBytes()))
}

// TestStreamSetChunkSizeInvalid checks that invalid chunk sizes are rejected
// with a clear error and that neither the current chunk size nor the ongoing
// session is affected.
func TestStreamSetChunkSizeInvalid(t *testing.T) {
	rwc := &rwcMock{}
	conn := newConn(rwc, &ConnConfig{
		ControlState: StreamControlStateConfig{
			MaxChunkSize: 1024,
		},
	})
	defer closeStreamer(t, conn)
	stream := newStream(1, conn)

	oldChunkSize := conn.streamer.SelfState().ChunkSize()

	// Zero is invalid
	err := stream.SetChunkSize(0)
	require.EqualError(t, err, "Failed to change chunk size: Chunk size must be greater than 0: Value = 0")
	require.Equal(t, oldChunkSize, conn.streamer.SelfState().ChunkSize())

	// Exceeding the configured max chunk size is invalid
	err = stream.SetChunkSize(1025)
	require.EqualError(t, err, "Failed to change chunk size: Exceeded configured max chunk size: Limit = 1024, Value = 1025")
	require.Equal(t, oldChunkSize, conn.streamer.SelfState().ChunkSize())

	// The session is still usable: messages can be written and read back
	videoContent := bytes.Repeat([]byte("v"), 300)
	writeStreamMessageAndWait(t, stream, 8, 100, &message.VideoMessage{
		Payload: bytes.NewReader(videoContent),
	})

	readerStreamer := NewChunkStreamer(bytes.NewReader(rwc.Bytes()), ioutil.Discard, nil)
	defer readerStreamer.Close()

	var cmsg ChunkMessage
	_, timestamp, err := readerStreamer.Read(&cmsg)
	require.Nil(t, err)
	require.Equal(t, uint32(100), timestamp)
	actualContent, err := ioutil.ReadAll(cmsg.Message.(*message.VideoMessage).Payload)
	require.Nil(t, err)
	require.Equal(t, videoContent, actualContent)
}

// TestStreamSetChunkSizeSameSize checks that setting the current chunk size is
// a no-op and does not emit any message.
func TestStreamSetChunkSizeSameSize(t *testing.T) {
	rwc := &rwcMock{}
	conn := newConn(rwc, nil)
	defer closeStreamer(t, conn)
	stream := newStream(1, conn)

	err := stream.SetChunkSize(conn.streamer.SelfState().ChunkSize())
	require.Nil(t, err)

	require.Equal(t, 0, rwc.Len())
}

// TestStreamHandlerSetChunkSize checks that a chunk size notified by the peer
// is applied to subsequent chunks, and that an invalid one is ignored without
// breaking the session.
func TestStreamHandlerSetChunkSize(t *testing.T) {
	rwc := &rwcMock{}
	conn := newConn(rwc, &ConnConfig{
		ControlState: StreamControlStateConfig{
			MaxChunkSize: 1024,
		},
	})
	defer closeStreamer(t, conn)
	stream := newStream(42, conn)

	// A valid notification is applied
	err := stream.handler.Handle(2, 0, &message.SetChunkSize{ChunkSize: 512})
	require.Nil(t, err)
	require.Equal(t, uint32(512), conn.streamer.PeerState().ChunkSize())

	// An invalid notification is ignored, and the session is kept
	err = stream.handler.Handle(2, 0, &message.SetChunkSize{ChunkSize: 2048})
	require.Nil(t, err)
	require.Equal(t, uint32(512), conn.streamer.PeerState().ChunkSize())
}

// writeStreamMessageAndWait writes a message and blocks until it has been
// written out completely.
func writeStreamMessageAndWait(t *testing.T, stream *Stream, chunkStreamID int, timestamp uint32, msg message.Message) {
	doneCh := make(chan struct{})
	err := stream.streamer().writeWithCompletionCallback(
		context.Background(),
		chunkStreamID,
		timestamp,
		&ChunkMessage{
			StreamID: stream.streamID,
			Message:  msg,
		},
		func() { close(doneCh) },
	)
	require.Nil(t, err)
	<-doneCh
}

func closeStreamer(t *testing.T, conn *Conn) {
	require.Nil(t, conn.streamer.Close())
	<-conn.streamer.Done()
}
