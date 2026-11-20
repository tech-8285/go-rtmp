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

// PlayRejectedError is returned by Stream.Play when a server replies to a play
// request with an error-level onStatus notification (for example
// NetStream.Play.Failed).
type PlayRejectedError struct {
	StreamID uint32
	Status   *message.NetStreamOnStatus
}

func (err *PlayRejectedError) Error() string {
	return fmt.Sprintf(
		"Play is rejected: StreamID = %d, Status = %#v",
		err.StreamID,
		err.Status,
	)
}
