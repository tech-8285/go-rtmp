//
// Copyright (c) 2018- yutopp (yutopp@gmail.com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at  https://www.boost.org/LICENSE_1_0.txt)
//

package message

import (
	"bytes"
)

type testCase struct {
	Name string
	TypeID
	Value  Message
	Binary []byte
}

var testCases = []testCase{
	{
		Name:   "SetChunkSize",
		TypeID: TypeIDSetChunkSize,
		Value: &SetChunkSize{
			ChunkSize: 1024,
		},
		Binary: []byte{
			// ChunkSize 1024 (*31bit*, BigEndian)
			0x00, 0x00, 0x04, 0x00,
		},
	},
	{
		Name:   "AbortMessage",
		TypeID: TypeIDAbortMessage,
		Value: &AbortMessage{
			ChunkStreamID: 1024,
		},
		Binary: []byte{
			// ChunkStreamID 1024 (32bit, BigEndian)
			0x00, 0x00, 0x04, 0x00,
		},
	},
	{
		Name:   "Ack",
		TypeID: TypeIDAck,
		Value: &Ack{
			SequenceNumber: 1024,
		},
		Binary: []byte{
			// SequenceNumber 1024 (32bit, BigEndian)
			0x00, 0x00, 0x04, 0x00,
		},
	},
	// TODO: UserCtrl
	{
		Name:   "WinAckSize",
		TypeID: TypeIDWinAckSize,
		Value: &WinAckSize{
			Size: 1024,
		},
		Binary: []byte{
			// Size 1024 (32bit, BigEndian)
			0x00, 0x00, 0x04, 0x00,
		},
	},
	{
		Name:   "SetPeerBandwidth",
		TypeID: TypeIDSetPeerBandwidth,
		Value: &SetPeerBandwidth{
			Size:  1024,
			Limit: LimitTypeSoft,
		},
		Binary: []byte{
			// Size 1024 (32bit, BigEndian)
			0x00, 0x00, 0x04, 0x00,
			// Limit Type 1(LimitTypeSoft, 8bit)
			0x01,
		},
	},
	{
		Name:   "AudioMessage",
		TypeID: TypeIDAudioMessage,
		Value: &AudioMessage{
			Payload: bytes.NewReader([]byte("audio data")),
		},
		Binary: []byte("audio data"),
	},
	{
		Name:   "VideoMessage",
		TypeID: TypeIDVideoMessage,
		Value: &VideoMessage{
			Payload: bytes.NewReader([]byte("video data")),
		},
		Binary: []byte("video data"),
	},
	// TODO: DataMessageAMF3
	{
		Name:   "DataMessageAMF0",
		TypeID: TypeIDDataMessageAMF0,
		Value: &DataMessage{
			Name:     "test",
			Encoding: EncodingTypeAMF0,
			Body:     bytes.NewReader([]byte("test")),
		},
		Binary: []byte{
			// Name: AMF0 / string marker
			0x02,
			// Name: AMF0 / string Length 4
			0x00, 0x04,
			// Name: AMF0 / "test" string
			0x74, 0x65, 0x73, 0x74,
			// RAW Binary: test
			0x74, 0x65, 0x73, 0x74,
		},
	},
	// TODO: TypeIDSharedObjectMessageAMF3
	// TODO: TypeIDCommandMessageAMF3
	// TODO: TypeIDSharedObjectMessageAMF0
	{
		Name:   "CommandMessageAMF0",
		TypeID: TypeIDCommandMessageAMF0,
		Value: &CommandMessage{
			CommandName:   "_result",
			TransactionID: 10,
			Encoding:      EncodingTypeAMF0,
			Body:          bytes.NewReader([]byte("test")),
		},
		Binary: []byte{
			// CommandName: AMF0 / string marker
			0x02,
			// CommandName: AMF0 / string Length
			0x00, 0x07,
			// CommandName: AMF0 / "_result" string
			0x5f, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74,
			// TransactionID: AMF0 / number marker
			0x00,
			// TransactionID: AMF0 / 10 number
			0x40, 0x24, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			// RAW Binary: test
			0x74, 0x65, 0x73, 0x74,
		},
	},
	{
		Name:   "AggregateMessage",
		TypeID: TypeIDAggregateMessage,
		Value: &AggregateMessage{
			SubMessages: []AggregateSubMessage{
				{
					Message: &AudioMessage{
						Payload: bytes.NewReader([]byte("audio data")),
					},
					Timestamp: 0x01000010,
				},
				{
					Message: &VideoMessage{
						Payload: bytes.NewReader([]byte("video data")),
					},
					Timestamp: 0x01000030,
					StreamID:  42,
				},
			},
		},
		Binary: []byte{
			// Audio tag
			// TagType = 8
			0x08,
			// DataSize = 10 (24bits, BigEndian)
			0x00, 0x00, 0x0a,
			// Timestamp = 0x01000010 (lower 24bits + upper 8bits)
			0x00, 0x00, 0x10, 0x01,
			// StreamID = 0 (24bits, BigEndian)
			0x00, 0x00, 0x00,
			// Payload: "audio data"
			0x61, 0x75, 0x64, 0x69, 0x6f, 0x20, 0x64, 0x61, 0x74, 0x61,
			// PreviousTagSize = 11 + 10
			0x00, 0x00, 0x00, 0x15,

			// Video tag
			// TagType = 9
			0x09,
			// DataSize = 10 (24bits, BigEndian)
			0x00, 0x00, 0x0a,
			// Timestamp = 0x01000030 (lower 24bits + upper 8bits)
			0x00, 0x00, 0x30, 0x01,
			// StreamID = 42 (24bits, BigEndian)
			0x00, 0x00, 0x2a,
			// Payload: "video data"
			0x76, 0x69, 0x64, 0x65, 0x6f, 0x20, 0x64, 0x61, 0x74, 0x61,
			// PreviousTagSize = 11 + 10
			0x00, 0x00, 0x00, 0x15,
		},
	},
}
