//
// Copyright (c) 2018- yutopp (yutopp@gmail.com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at  https://www.boost.org/LICENSE_1_0.txt)
//

package message

import (
	"io"
)

type TypeID byte

const (
	TypeIDSetChunkSize            TypeID = 1
	TypeIDAbortMessage            TypeID = 2
	TypeIDAck                     TypeID = 3
	TypeIDUserCtrl                TypeID = 4
	TypeIDWinAckSize              TypeID = 5
	TypeIDSetPeerBandwidth        TypeID = 6
	TypeIDAudioMessage            TypeID = 8
	TypeIDVideoMessage            TypeID = 9
	TypeIDDataMessageAMF3         TypeID = 15
	TypeIDSharedObjectMessageAMF3 TypeID = 16
	TypeIDCommandMessageAMF3      TypeID = 17
	TypeIDDataMessageAMF0         TypeID = 18
	TypeIDSharedObjectMessageAMF0 TypeID = 19
	TypeIDCommandMessageAMF0      TypeID = 20
	TypeIDAggregateMessage        TypeID = 22
)

// Message
type Message interface {
	TypeID() TypeID
}

// SetChunkSize (1)
type SetChunkSize struct {
	ChunkSize uint32
}

func (m *SetChunkSize) TypeID() TypeID {
	return TypeIDSetChunkSize
}

// AbortMessage (2)
type AbortMessage struct {
	ChunkStreamID uint32
}

func (m *AbortMessage) TypeID() TypeID {
	return TypeIDAbortMessage
}

// Ack (3)
type Ack struct {
	SequenceNumber uint32
}

func (m *Ack) TypeID() TypeID {
	return TypeIDAck
}

// UserCtrl (4)
type UserCtrl struct {
	Event UserCtrlEvent
}

func (m *UserCtrl) TypeID() TypeID {
	return TypeIDUserCtrl
}

// WinAckSize (5)
type WinAckSize struct {
	Size int32
}

func (m *WinAckSize) TypeID() TypeID {
	return TypeIDWinAckSize
}

// SetPeerBandwidth (6)
type LimitType uint8

const (
	LimitTypeHard LimitType = iota
	LimitTypeSoft
	LimitTypeDynamic
)

type SetPeerBandwidth struct {
	Size  int32
	Limit LimitType
}

func (m *SetPeerBandwidth) TypeID() TypeID {
	return TypeIDSetPeerBandwidth
}

// AudioMessage(8)
type AudioMessage struct {
	Payload io.Reader
}

func (m *AudioMessage) TypeID() TypeID {
	return TypeIDAudioMessage
}

// VideoMessage(9)
type VideoMessage struct {
	Payload io.Reader
}

func (m *VideoMessage) TypeID() TypeID {
	return TypeIDVideoMessage
}

// DataMessage (15, 18)
type DataMessage struct {
	Name     string
	Encoding EncodingType
	Body     io.Reader
}

func (m *DataMessage) TypeID() TypeID {
	switch m.Encoding {
	case EncodingTypeAMF3:
		return TypeIDDataMessageAMF3
	case EncodingTypeAMF0:
		return TypeIDDataMessageAMF0
	default:
		panic("Unreachable")
	}
}

// SharedObjectMessage (16, 19)
type SharedObjectMessage struct {
}

type SharedObjectMessageAMF3 struct {
	SharedObjectMessage
}

func (m *SharedObjectMessageAMF3) TypeID() TypeID {
	return TypeIDSharedObjectMessageAMF3
}

type SharedObjectMessageAMF0 struct {
	SharedObjectMessage
}

func (m *SharedObjectMessageAMF0) TypeID() TypeID {
	return TypeIDSharedObjectMessageAMF0
}

// CommandMessage (17, 20)
type CommandMessage struct {
	CommandName   string
	TransactionID int64
	Encoding      EncodingType
	Body          io.Reader
}

func (m *CommandMessage) TypeID() TypeID {
	switch m.Encoding {
	case EncodingTypeAMF3:
		return TypeIDCommandMessageAMF3
	case EncodingTypeAMF0:
		return TypeIDCommandMessageAMF0
	default:
		panic("Unreachable")
	}
}

// AggregateMessage (22)
//
// An aggregate message bundles several sub-messages (audio, video and script
// data) into a single message. Each sub-message keeps its own type, message
// stream ID, timestamp and payload, and must be delivered as if it was sent
// independently.
type AggregateMessage struct {
	// SubMessages are the sub-messages contained in the aggregate message,
	// in the same order as they appear in the message body.
	SubMessages []AggregateSubMessage
}

// TimestampAt returns the timestamp of the i-th sub-message converted onto the
// timeline of the stream which carries the aggregate message.
//
// Sub-message timestamps are relative to the first sub-message, so they are
// rebased onto the timestamp of the aggregate message itself. This is stable
// across multiple aggregate messages because each one is rebased by its own
// base timestamp.
func (m *AggregateMessage) TimestampAt(baseTimestamp uint32, i int) uint32 {
	if i < 0 || i >= len(m.SubMessages) {
		return baseTimestamp
	}

	return baseTimestamp + (m.SubMessages[i].Timestamp - m.SubMessages[0].Timestamp)
}

func (m *AggregateMessage) TypeID() TypeID {
	return TypeIDAggregateMessage
}

// AggregateSubMessage is a single sub-message embedded in an AggregateMessage.
type AggregateSubMessage struct {
	// Message is the decoded sub-message. Its payload is backed by an
	// independent buffer, so it can be read safely after the aggregate message
	// has been consumed.
	Message Message

	// Timestamp is the raw timestamp carried by the sub-message. It is
	// relative to the first sub-message of the aggregate.
	Timestamp uint32

	// StreamID is the message stream ID carried by the sub-message.
	StreamID uint32
}
