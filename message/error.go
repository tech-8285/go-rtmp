//
// Copyright (c) 2018- yutopp (yutopp@gmail.com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at  https://www.boost.org/LICENSE_1_0.txt)
//

package message

import (
	"fmt"
)

type UnknownDataBodyDecodeError struct {
	Name string
	Objs []interface{}
}

func (e *UnknownDataBodyDecodeError) Error() string {
	return fmt.Sprintf("UnknownDataBodyDecodeError: Name = %s, Objs = %+v", e.Name, e.Objs)
}

type UnknownCommandBodyDecodeError struct {
	Name          string
	TransactionID int64
	Objs          []interface{}
}

func (e *UnknownCommandBodyDecodeError) Error() string {
	return fmt.Sprintf("UnknownCommandMessageDecodeError: Name = %s, TransactionID = %d, Objs = %+v",
		e.Name,
		e.TransactionID,
		e.Objs,
	)
}

// AggregateMessageDecodeError reports a structural corruption detected while
// splitting an aggregate message. It locates the offending sub-message by its
// index and its byte offset from the beginning of the aggregate body.
type AggregateMessageDecodeError struct {
	// SubMessageIndex is the zero-based index of the sub-message which could not
	// be parsed. It equals the number of sub-messages which were successfully
	// parsed before the failure.
	SubMessageIndex int

	// Offset is the byte offset, from the beginning of the aggregate message
	// body, at which the offending sub-message starts.
	Offset int

	// Reason describes what is wrong with the sub-message.
	Reason string

	// Err is the underlying cause, if any.
	Err error
}

func (e *AggregateMessageDecodeError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf(
			"AggregateMessageDecodeError: SubMessageIndex = %d, Offset = %d, Reason = %s, Err = %+v",
			e.SubMessageIndex,
			e.Offset,
			e.Reason,
			e.Err,
		)
	}

	return fmt.Sprintf(
		"AggregateMessageDecodeError: SubMessageIndex = %d, Offset = %d, Reason = %s",
		e.SubMessageIndex,
		e.Offset,
		e.Reason,
	)
}

func (e *AggregateMessageDecodeError) Unwrap() error {
	return e.Err
}
