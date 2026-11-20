//
// Copyright (c) 2018- yutopp (yutopp@gmail.com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at  https://www.boost.org/LICENSE_1_0.txt)
//

package rtmp

import (
	"bytes"
)

type ChunkStreamReader struct {
	basicHeader   chunkBasicHeader
	messageHeader chunkMessageHeader

	timestamp       uint32
	timestampDelta  uint32
	messageLength   uint32 // max, 24bits
	messageTypeID   byte
	messageStreamID uint32

	// State of the extended timestamp field of the previous chunk on this
	// chunk stream. A fmt=3 chunk repeats the 32bits extended timestamp
	// field when the previous chunk carried one, so both the reader and
	// the writer have to track it per chunk stream to keep the message
	// timeline consistent.
	hasExtendedTimestamp bool
	extendedTimestamp    uint32

	buf       bytes.Buffer
	completed bool
}

func (r *ChunkStreamReader) Read(b []byte) (int, error) {
	return r.buf.Read(b)
}
