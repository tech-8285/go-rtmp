//
// Copyright (c) 2018- yutopp (yutopp@gmail.com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at  https://www.boost.org/LICENSE_1_0.txt)
//

package rtmp

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/yutopp/go-rtmp/message"
)

var ErrClosed = errors.New("Server is closed")

// ErrMessageAborted is returned when a message could not be finished before its
// send deadline expired (or it was aborted explicitly).
// The remaining chunks are not sent and an AbortMessage is delivered to the peer
// so that it discards the unfinished message on the same chunk stream.
var ErrMessageAborted = errors.New("Message was aborted before completion")

type ConnectRejectedError struct {
	TransactionID int64
	Result        *message.NetConnectionConnectResult
}

func (err *ConnectRejectedError) Error() string {
	return fmt.Sprintf(
		"Connect is rejected: TransactionID = %d, Result = %#v",
		err.TransactionID,
		err.Result,
	)
}

type CreateStreamRejectedError struct {
	TransactionID int64
	Result        *message.NetConnectionCreateStreamResult
}

func (err *CreateStreamRejectedError) Error() string {
	return fmt.Sprintf(
		"CreateStream is rejected: TransactionID = %d, Result = %#v",
		err.TransactionID,
		err.Result,
	)
}
