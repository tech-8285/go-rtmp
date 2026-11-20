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
	"fmt"
	"io"

	"github.com/pkg/errors"
)

// FLV tag layout used by the body of an aggregate message:
//
//	TagType              (8bits)
//	DataSize             (24bits, BigEndian)
//	Timestamp            (24bits, BigEndian, lower 24bits)
//	TimestampExtended    (8bits, upper 8bits)
//	StreamID             (24bits, BigEndian)
//	Data                 (DataSize bytes)
//	PreviousTagSize      (32bits, BigEndian) = 11 + DataSize
const (
	flvTagTypeAudio      = 8
	flvTagTypeVideo      = 9
	flvTagTypeScriptData = 18

	flvTagHeaderSize   = 11
	flvTagPrevSizeSize = 4

	flvTagMaxDataSize = 0xffffff // 24bits
	flvTagMaxStreamID = 0xffffff // 24bits
)

// decodeAggregateBody splits an aggregate message body into sub-messages.
//
// Every sub-message is self-delimiting thanks to DataSize and PreviousTagSize,
// so a truncated or inconsistent boundary stops the parsing immediately with
// the location of the offending sub-message. Bytes following the valid part of
// the body are never reinterpreted as a new message.
func decodeAggregateBody(body []byte) ([]AggregateSubMessage, error) {
	subMessages := make([]AggregateSubMessage, 0, 1)

	offset := 0
	for offset < len(body) {
		subMsg, next, err := decodeAggregateSubMessage(body, offset, len(subMessages))
		if err != nil {
			return nil, err
		}

		subMessages = append(subMessages, *subMsg)
		offset = next
	}

	return subMessages, nil
}

func decodeAggregateSubMessage(body []byte, offset int, index int) (*AggregateSubMessage, int, error) {
	start := offset

	if len(body)-offset < flvTagHeaderSize {
		return nil, 0, &AggregateMessageDecodeError{
			SubMessageIndex: index,
			Offset:          start,
			Reason: fmt.Sprintf(
				"truncated sub-message header: need %d bytes, got %d",
				flvTagHeaderSize,
				len(body)-offset,
			),
		}
	}

	tagType := body[offset]
	dataSize := uint32(body[offset+1])<<16 | uint32(body[offset+2])<<8 | uint32(body[offset+3])
	timestamp := uint32(body[offset+4])<<16 | uint32(body[offset+5])<<8 | uint32(body[offset+6])
	timestamp |= uint32(body[offset+7]) << 24
	streamID := uint32(body[offset+8])<<16 | uint32(body[offset+9])<<8 | uint32(body[offset+10])

	offset += flvTagHeaderSize

	if uint32(len(body)-offset) < dataSize {
		return nil, 0, &AggregateMessageDecodeError{
			SubMessageIndex: index,
			Offset:          start,
			Reason: fmt.Sprintf(
				"truncated sub-message payload: need %d bytes, got %d",
				dataSize,
				len(body)-offset,
			),
		}
	}

	payload := body[offset : offset+int(dataSize)]
	offset += int(dataSize)

	if len(body)-offset < flvTagPrevSizeSize {
		return nil, 0, &AggregateMessageDecodeError{
			SubMessageIndex: index,
			Offset:          start,
			Reason: fmt.Sprintf(
				"truncated sub-message boundary field: need %d bytes, got %d",
				flvTagPrevSizeSize,
				len(body)-offset,
			),
		}
	}

	prevTagSize := binary.BigEndian.Uint32(body[offset : offset+flvTagPrevSizeSize])
	expectedPrevTagSize := uint32(flvTagHeaderSize) + dataSize
	if prevTagSize != expectedPrevTagSize {
		return nil, 0, &AggregateMessageDecodeError{
			SubMessageIndex: index,
			Offset:          start,
			Reason: fmt.Sprintf(
				"invalid sub-message boundary field: PreviousTagSize = %d, expected %d",
				prevTagSize,
				expectedPrevTagSize,
			),
		}
	}

	offset += flvTagPrevSizeSize

	subMsg, err := decodeAggregateSubMessagePayload(tagType, payload)
	if err != nil {
		return nil, 0, &AggregateMessageDecodeError{
			SubMessageIndex: index,
			Offset:          start,
			Reason:          fmt.Sprintf("invalid sub-message payload: tag type = %d", tagType),
			Err:             err,
		}
	}

	return &AggregateSubMessage{
		Message:   subMsg,
		Timestamp: timestamp,
		StreamID:  streamID,
	}, offset, nil
}

// decodeAggregateSubMessagePayload wraps a sub-message payload into a message.
//
// The payload is turned into an independent reader which is bounded to exactly
// DataSize bytes, so a consumer of one sub-message can never read the bytes of
// the following sub-messages.
func decodeAggregateSubMessagePayload(tagType byte, payload []byte) (Message, error) {
	switch tagType {
	case flvTagTypeAudio:
		return &AudioMessage{
			Payload: bytes.NewReader(payload),
		}, nil

	case flvTagTypeVideo:
		return &VideoMessage{
			Payload: bytes.NewReader(payload),
		}, nil

	case flvTagTypeScriptData:
		r := bytes.NewReader(payload)
		dec := NewDecoder(r)

		var msg Message
		if err := dec.Decode(TypeIDDataMessageAMF0, &msg); err != nil {
			return nil, errors.Wrap(err, "Failed to decode script data sub-message")
		}

		return msg, nil

	default:
		return nil, errors.Errorf("Unsupported FLV tag type for aggregate sub-message: %d", tagType)
	}
}

// encodeAggregateBody writes sub-messages as FLV tags.
func encodeAggregateBody(w io.Writer, subMessages []AggregateSubMessage) error {
	for i, subMsg := range subMessages {
		if err := encodeAggregateSubMessage(w, subMsg, i); err != nil {
			return err
		}
	}

	return nil
}

func encodeAggregateSubMessage(w io.Writer, subMsg AggregateSubMessage, index int) error {
	var payload bytes.Buffer
	var tagType byte

	switch msg := subMsg.Message.(type) {
	case *AudioMessage:
		tagType = flvTagTypeAudio
		if _, err := io.Copy(&payload, msg.Payload); err != nil {
			return errors.Wrapf(err, "Failed to encode aggregate sub-message: Index = %d", index)
		}

	case *VideoMessage:
		tagType = flvTagTypeVideo
		if _, err := io.Copy(&payload, msg.Payload); err != nil {
			return errors.Wrapf(err, "Failed to encode aggregate sub-message: Index = %d", index)
		}

	case *DataMessage:
		if msg.Encoding != EncodingTypeAMF0 {
			return errors.Errorf(
				"Unsupported encoding for aggregate sub-message: Index = %d, Encoding = %d",
				index,
				msg.Encoding,
			)
		}
		tagType = flvTagTypeScriptData

		amfEnc := NewAMFEncoder(&payload, msg.Encoding)
		if err := amfEnc.Encode(msg.Name); err != nil {
			return errors.Wrapf(err, "Failed to encode aggregate sub-message: Index = %d", index)
		}
		if _, err := io.Copy(&payload, msg.Body); err != nil {
			return errors.Wrapf(err, "Failed to encode aggregate sub-message: Index = %d", index)
		}

	default:
		return errors.Errorf(
			"Unsupported message type for aggregate sub-message: Index = %d, Type = %T",
			index,
			subMsg.Message,
		)
	}

	if payload.Len() > flvTagMaxDataSize {
		return errors.Errorf(
			"Too large aggregate sub-message: Index = %d, Size = %d",
			index,
			payload.Len(),
		)
	}
	if subMsg.StreamID > flvTagMaxStreamID {
		return errors.Errorf(
			"Invalid stream ID for aggregate sub-message: Index = %d, StreamID = %d",
			index,
			subMsg.StreamID,
		)
	}

	header := make([]byte, flvTagHeaderSize)
	header[0] = tagType

	dataSize := uint32(payload.Len())
	header[1] = byte(dataSize >> 16)
	header[2] = byte(dataSize >> 8)
	header[3] = byte(dataSize)

	header[4] = byte(subMsg.Timestamp >> 16)
	header[5] = byte(subMsg.Timestamp >> 8)
	header[6] = byte(subMsg.Timestamp)
	header[7] = byte(subMsg.Timestamp >> 24)

	header[8] = byte(subMsg.StreamID >> 16)
	header[9] = byte(subMsg.StreamID >> 8)
	header[10] = byte(subMsg.StreamID)

	if _, err := w.Write(header); err != nil {
		return err
	}
	if _, err := w.Write(payload.Bytes()); err != nil {
		return err
	}

	prevTagSize := make([]byte, flvTagPrevSizeSize)
	binary.BigEndian.PutUint32(prevTagSize, flvTagHeaderSize+dataSize)

	_, err := w.Write(prevTagSize)
	return err
}
