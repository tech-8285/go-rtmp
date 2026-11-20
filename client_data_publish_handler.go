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

var _ stateHandler = (*clientDataPublishHandler)(nil)

// clientDataPublishHandler handles messages on a client-side stream which is
// publishing to a server. Publishers mainly send media, however servers may
// still answer with onStatus notifications (e.g. Publish.BadName).
//
//	transitions:
//	  | onStatus(terminal) -> clientDataInactiveHandler
//	  | _                  -> self
type clientDataPublishHandler struct {
	sh *streamHandler
}

func (h *clientDataPublishHandler) onMessage(
	chunkStreamID int,
	timestamp uint32,
	msg message.Message,
) error {
	return internal.ErrPassThroughMsg
}

func (h *clientDataPublishHandler) onData(
	chunkStreamID int,
	timestamp uint32,
	dataMsg *message.DataMessage,
	body interface{},
) error {
	return internal.ErrPassThroughMsg
}

func (h *clientDataPublishHandler) onCommand(
	chunkStreamID int,
	timestamp uint32,
	cmdMsg *message.CommandMessage,
	body interface{},
) error {
	switch cmd := body.(type) {
	case *message.NetStreamOnStatus:
		switch cmd.InfoObject.Code {
		case message.NetStreamOnStatusCodePublishBadName,
			message.NetStreamOnStatusCodePublishFailed,
			message.NetStreamOnStatusCodeUnpublishSuccess:
			h.sh.ChangeState(streamStateClientInactive)
		default:
			// Stay in the publishing state for Publish.Start and others.
		}
		return h.sh.stream.handlePlayPublishStatus(cmd)

	default:
		return internal.ErrPassThroughMsg
	}
}
