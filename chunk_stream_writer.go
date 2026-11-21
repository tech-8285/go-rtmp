//
// Copyright (c) 2018- yutopp (yutopp@gmail.com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at  https://www.boost.org/LICENSE_1_0.txt)
//

package rtmp

import (
	"context"
	"math"
	"sync"
)

type ChunkStreamWriter struct {
	ChunkStreamReader

	doneCh   chan struct{}
	closeCh  chan struct{}
	lastErr  error
	aqM      sync.Mutex
	newChunk bool

	// stateM guards inFlight and epoch, the scheduling state shared between the
	// caller goroutine (Sched/Wait) and the scheduler goroutine (Run/abort).
	stateM   sync.Mutex
	inFlight bool
	epoch    uint64
}

func (w *ChunkStreamWriter) Write(b []byte) (int, error) {
	return w.buf.Write(b)
}

func (w *ChunkStreamWriter) Wait(ctx context.Context) error {
	w.aqM.Lock()
	defer w.aqM.Unlock()

	select {
	case <-w.doneCh:
		err := w.lastErr

		// Re-arm the writer regardless of the previous outcome so that the same
		// chunk stream can carry a new message. An aborted previous message is
		// consumed transparently here: callers acquiring the writer for a new
		// message must not inherit that failure.
		w.stateM.Lock()
		w.inFlight = false
		w.stateM.Unlock()
		w.lastErr = nil
		w.doneCh = make(chan struct{})

		if err == ErrMessageAborted {
			return nil
		}
		return err

	case <-w.closeCh:
		return w.lastErr

	case <-ctx.Done():
		return ctx.Err()
	}
}

// resetForNewMessage clears every piece of state belonging to the current
// (possibly unfinished) message so that the next message starts from a clean
// slate: its payload buffer is dropped and the cached header is reset, which
// forces a type-0 (full) header on the next chunk.
// Must be called from the scheduler goroutine.
func (w *ChunkStreamWriter) resetForNewMessage() {
	w.buf.Reset()
	w.completed = false

	w.basicHeader = chunkBasicHeader{
		chunkStreamID: w.basicHeader.chunkStreamID,
	}
	w.messageHeader = chunkMessageHeader{
		timestamp: math.MaxUint32, // initial state will be updated by writer.timestamp
	}

	w.timestamp = 0
	w.timestampDelta = 0
	w.messageLength = 0
	w.messageTypeID = 0
	w.messageStreamID = 0
	w.newChunk = true
}
