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

var _ stateHandler = (*clientControlConnectedHandler)(nil)

// clientControlConnectedHandler handles control messages (stream id 0) from a
// server after the connect command has been accepted.
//
//	transitions:
//	  | _ -> self
type clientControlConnectedHandler struct {
	sh *streamHandler
}

func (h *clientControlConnectedHandler) onMessage(
	chunkStreamID int,
	timestamp uint32,
	msg message.Message,
) error {
	return internal.ErrPassThroughMsg
}

func (h *clientControlConnectedHandler) onData(
	chunkStreamID int,
	timestamp uint32,
	dataMsg *message.DataMessage,
	body interface{},
) error {
	return internal.ErrPassThroughMsg
}

func (h *clientControlConnectedHandler) onCommand(
	chunkStreamID int,
	timestamp uint32,
	cmdMsg *message.CommandMessage,
	body interface{},
) error {
	switch body.(type) {
	// "_result" / "_error" are consumed by the transaction manager in
	// streamHandler.handleCommand before reaching this handler.
	case *message.NetConnectionConnectResult:
		return nil
	default:
		return internal.ErrPassThroughMsg
	}
}
