//
// Copyright (c) 2018- yutopp (yutopp@gmail.com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at  https://www.boost.org/LICENSE_1_0.txt)
//

package message

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yutopp/go-amf0"
)

func encodeAMF0(t *testing.T, values ...interface{}) *bytes.Reader {
	t.Helper()
	buf := new(bytes.Buffer)
	enc := amf0.NewEncoder(buf)
	for _, v := range values {
		require.Nil(t, enc.Encode(v))
	}
	return bytes.NewReader(buf.Bytes())
}

func TestDecodeCmdMessageOnStatus(t *testing.T) {
	r := encodeAMF0(t,
		nil, // command object
		amf0.ECMAArray{
			"level":       "status",
			"code":        "NetStream.Play.Start",
			"description": "Play succeeded.",
		},
	)
	d := amf0.NewDecoder(r)

	var v AMFConvertible
	err := CmdBodyDecoderFor("onStatus", 0)(r, d, &v)
	require.Nil(t, err)
	require.Equal(t, &NetStreamOnStatus{
		InfoObject: NetStreamOnStatusInfoObject{
			Level:       NetStreamOnStatusLevelStatus,
			Code:        NetStreamOnStatusCodePlayStart,
			Description: "Play succeeded.",
		},
	}, v)
}

func TestDecodeCmdMessageOnStatusError(t *testing.T) {
	r := encodeAMF0(t,
		nil,
		amf0.ECMAArray{
			"level": "error",
			"code":  "NetStream.Play.Failed",
		},
	)
	d := amf0.NewDecoder(r)

	var v AMFConvertible
	err := CmdBodyDecoderFor("onStatus", 0)(r, d, &v)
	require.Nil(t, err)

	status := v.(*NetStreamOnStatus)
	require.Equal(t, NetStreamOnStatusLevelError, status.InfoObject.Level)
	require.Equal(t, NetStreamOnStatusCodePlayFailed, status.InfoObject.Code)
}

func TestDecodeCmdMessageOnStatusMissingInfoObject(t *testing.T) {
	r := encodeAMF0(t, nil)
	d := amf0.NewDecoder(r)

	var v AMFConvertible
	err := CmdBodyDecoderFor("onStatus", 0)(r, d, &v)
	require.Error(t, err)
}

func TestDecodeDataMessageOnMetaData(t *testing.T) {
	metadata := amf0.ECMAArray{
		"duration": 12.5,
		"width":    float64(640),
	}
	r := encodeAMF0(t, metadata)
	d := amf0.NewDecoder(r)

	var v AMFConvertible
	err := DataBodyDecoderFor("onMetaData")(r, d, &v)
	require.Nil(t, err)

	frame := v.(*NetStreamSetDataFrame)
	decoded, ok := frame.AmfData.(amf0.ECMAArray)
	require.True(t, ok)
	require.Equal(t, 12.5, decoded["duration"])
	require.Equal(t, float64(640), decoded["width"])
	require.NotEmpty(t, frame.Payload)
}

func TestNetStreamPlayToArgs(t *testing.T) {
	play := &NetStreamPlay{
		StreamName: "theStream",
		Start:      -2,
	}

	args, err := play.ToArgs(EncodingTypeAMF0)
	require.Nil(t, err)
	require.Equal(t, []interface{}{nil, "theStream", int64(-2)}, args)

	// Round trip through FromArgs.
	var got NetStreamPlay
	require.Nil(t, got.FromArgs(args...))
	require.Equal(t, "theStream", got.StreamName)
	require.Equal(t, int64(-2), got.Start)
}

func TestNetStreamOnStatusToArgs(t *testing.T) {
	status := &NetStreamOnStatus{
		InfoObject: NetStreamOnStatusInfoObject{
			Level:       NetStreamOnStatusLevelStatus,
			Code:        NetStreamOnStatusCodePlayComplete,
			Description: "done",
		},
	}

	args, err := status.ToArgs(EncodingTypeAMF0)
	require.Nil(t, err)
	require.Len(t, args, 2)
	require.Nil(t, args[0])

	info, ok := args[1].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, NetStreamOnStatusCodePlayComplete, info["code"])
}
