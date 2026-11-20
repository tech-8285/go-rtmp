package main

import (
	"io"

	log "github.com/sirupsen/logrus"

	"github.com/yutopp/go-rtmp"
	rtmpmsg "github.com/yutopp/go-rtmp/message"
)

const (
	chunkSize = 128
)

// playHandler receives audio, video and metadata of a pulled stream with
// their original timestamps.
type playHandler struct {
	rtmp.DefaultHandler
}

func (h *playHandler) OnSetDataFrame(timestamp uint32, data *rtmpmsg.NetStreamSetDataFrame) error {
	log.Infof("onMetaData: timestamp = %d, metadata = %#v", timestamp, data.AmfData)
	return nil
}

func (h *playHandler) OnAudio(timestamp uint32, payload io.Reader) error {
	buf, err := io.ReadAll(payload)
	if err != nil {
		return err
	}
	log.Infof("audio: timestamp = %d, bytes = %d", timestamp, len(buf))
	return nil
}

func (h *playHandler) OnVideo(timestamp uint32, payload io.Reader) error {
	buf, err := io.ReadAll(payload)
	if err != nil {
		return err
	}
	log.Infof("video: timestamp = %d, bytes = %d", timestamp, len(buf))
	return nil
}

func (h *playHandler) OnClose() {
	log.Info("connection closed")
}

func main() {
	client, err := rtmp.Dial("rtmp", "localhost:1935", &rtmp.ConnConfig{
		Handler: &playHandler{},
		Logger:  log.StandardLogger(),
	})
	if err != nil {
		log.Fatalf("Failed to dial: %+v", err)
	}
	defer client.Close()
	log.Infof("Client created")

	if err := client.Connect(nil); err != nil {
		log.Fatalf("Failed to connect: Err=%+v", err)
	}
	log.Infof("connected")

	stream, err := client.CreateStream(nil, chunkSize)
	if err != nil {
		log.Fatalf("Failed to create stream: Err=%+v", err)
	}
	defer func() {
		// Clean up server-side state before the connection goes away.
		if err := stream.CloseStream(); err != nil {
			log.Warnf("Failed to close stream: %+v", err)
		}
	}()

	// Start pulling the stream. Play blocks until the server accepts or
	// rejects the request; afterwards media is delivered through the handler.
	status, err := stream.Play(&rtmpmsg.NetStreamPlay{
		StreamName: "testtesttesttest",
		Start:      -2, // live edge
	})
	if err != nil {
		log.Fatalf("Failed to play stream: Err=%+v", err)
	}
	log.Infof("playing: status = %s", status.InfoObject.Code)

	// Media is handled asynchronously by playHandler. Block forever; a real
	// application would wait on a shutdown signal.
	select {}
}
