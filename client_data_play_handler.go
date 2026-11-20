//
// Copyright (c) 2018- yutopp (yutopp@gmail.com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at  https://www.boost.org/LICENSE_1_0.txt)
//

package rtmp

import (
	"bytes"
	"io"

	"github.com/yutopp/go-rtmp/internal"
	"github.com/yutopp/go-rtmp/message"
)

var _ stateHandler = (*clientDataPlayHandler)(nil)

// clientDataPlayHandler handles messages delivered by a server to a client
// which is playing (pulling) a stream.
//
// Audio, video and metadata messages are handed to the user handler with the
// original timestamps kept intact. onStatus notifications drive the play
// lifecycle (start / complete / failure / reset) and never cross into other
// streams because routing is keyed by the message stream id.
//
//	transitions:
//	  | onStatus(terminal) -> clientDataInactiveHandler
//	  | _                  -> self
type clientDataPlayHandler struct {
	sh *streamHandler
}

func (h *clientDataPlayHandler) onMessage(
	chunkStreamID int,
	timestamp uint32,
	msg message.Message,
) error {
	switch msg := msg.(type) {
	case *message.AudioMessage:
		// The payload reader is owned by the chunk streamer and is reused for
		// the following message, therefore materialize it before invoking the
		// user callback so callers can keep the bytes if needed.
		payload, err := readAllPayload(msg.Payload)
		if err != nil {
			return err
		}
		return h.sh.stream.userHandler().OnAudio(timestamp, bytes.NewReader(payload))

	case *message.VideoMessage:
		payload, err := readAllPayload(msg.Payload)
		if err != nil {
			return err
		}
		return h.sh.stream.userHandler().OnVideo(timestamp, bytes.NewReader(payload))

	case *message.UserCtrl:
		// Stream EOF is the server's signal that playback finished. Treat it
		// as a terminal status, everything else is reported as unknown.
		if _, ok := msg.Event.(*message.UserCtrlEventStreamEOF); ok {
			h.sh.stream.notifyPlayStatus(&message.NetStreamOnStatus{
				InfoObject: message.NetStreamOnStatusInfoObject{
					Level:       message.NetStreamOnStatusLevelStatus,
					Code:        message.NetStreamOnStatusCodePlayComplete,
					Description: "Server reported stream EOF.",
				},
			})
			h.sh.ChangeState(streamStateClientInactive)
			return nil
		}
		return internal.ErrPassThroughMsg

	default:
		return internal.ErrPassThroughMsg
	}
}

func (h *clientDataPlayHandler) onData(
	chunkStreamID int,
	timestamp uint32,
	dataMsg *message.DataMessage,
	body interface{},
) error {
	switch data := body.(type) {
	case *message.NetStreamSetDataFrame:
		// Metadata ("onMetaData" / "@setDataFrame") follows the same timeline
		// as audio and video messages.
		return h.sh.stream.userHandler().OnSetDataFrame(timestamp, data)

	default:
		return internal.ErrPassThroughMsg
	}
}

func (h *clientDataPlayHandler) onCommand(
	chunkStreamID int,
	timestamp uint32,
	cmdMsg *message.CommandMessage,
	body interface{},
) error {
	switch cmd := body.(type) {
	case *message.NetStreamOnStatus:
		switch cmd.InfoObject.Code {
		case message.NetStreamOnStatusCodePlayComplete,
			message.NetStreamOnStatusCodePlayFailed,
			message.NetStreamOnStatusCodeConnectFailed,
			message.NetStreamOnStatusCodeMuticastStreamReset:
			// Terminal status: leave the playing state so that the stream can
			// be reused or closed cleanly before reconnecting.
			h.sh.ChangeState(streamStateClientInactive)
		default:
			// Non-terminal status (e.g. Play.Start delivered late): stay in
			// the playing state.
		}
		return h.sh.stream.handlePlayPublishStatus(cmd)

	default:
		return internal.ErrPassThroughMsg
	}
}

func readAllPayload(r io.Reader) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
