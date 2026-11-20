//
// Copyright (c) 2018- yutopp (yutopp@gmail.com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at  https://www.boost.org/LICENSE_1_0.txt)
//

package rtmp

import (
	"github.com/yutopp/go-rtmp/internal"
	"github.com/yutopp/go-rtmp/message"
)

var _ stateHandler = (*clientDataInactiveHandler)(nil)

// clientDataInactiveHandler handles messages of a client-side data stream
// (stream id != 0) which has not started playing or publishing yet.
//
//	transitions:
//	  | onStatus(NetStream.Play.*)    -> clientDataPlayHandler
//	  | onStatus(NetStream.Publish.*) -> clientDataPublishHandler
//	  | _                             -> self
type clientDataInactiveHandler struct {
	sh *streamHandler
}

func (h *clientDataInactiveHandler) onMessage(
	chunkStreamID int,
	timestamp uint32,
	msg message.Message,
) error {
	return internal.ErrPassThroughMsg
}

func (h *clientDataInactiveHandler) onData(
	chunkStreamID int,
	timestamp uint32,
	dataMsg *message.DataMessage,
	body interface{},
) error {
	return internal.ErrPassThroughMsg
}

func (h *clientDataInactiveHandler) onCommand(
	chunkStreamID int,
	timestamp uint32,
	cmdMsg *message.CommandMessage,
	body interface{},
) error {
	switch cmd := body.(type) {
	case *message.NetStreamOnStatus:
		return h.sh.stream.handlePlayPublishStatus(cmd)

	default:
		return internal.ErrPassThroughMsg
	}
}
