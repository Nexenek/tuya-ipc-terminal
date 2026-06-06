package onvif

import (
	"context"
	"fmt"
	"net"
	"time"

	onvifsrv "github.com/0x524a/onvif-go/server"
	"tuya-ipc-terminal/pkg/core"
	"tuya-ipc-terminal/pkg/storage"
)

type ONVIFServer struct {
	port           int
	rtspPort       int
	storageManager *storage.StorageManager
	srvInstance    *onvifsrv.Server
	cancelFunc     context.CancelFunc
}

func NewONVIFServer(port int, rtspPort int, sm *storage.StorageManager) *ONVIFServer {
	return &ONVIFServer{
		port:           port,
		rtspPort:       rtspPort,
		storageManager: sm,
	}
}

func (s *ONVIFServer) Start(ctx context.Context) error {
	cameras, err := s.storageManager.GetAllCameras()
	if err != nil {
		return fmt.Errorf("failed to load cameras for ONVIF server: %w", err)
	}

	var profiles []onvifsrv.ProfileConfig
	localIP := getLocalIP()

	for idx, cam := range cameras {
		hdToken := fmt.Sprintf("profile_%s_hd", cam.DeviceID)
		profiles = append(profiles, onvifsrv.ProfileConfig{
			Token: hdToken,
			Name:  cam.DeviceName + " HD",
			VideoSource: onvifsrv.VideoSourceConfig{
				Token:      fmt.Sprintf("source_%d", idx),
				Name:       cam.DeviceName + " Source",
				Resolution: onvifsrv.Resolution{Width: 1920, Height: 1080},
				Framerate:  30,
			},
			VideoEncoder: onvifsrv.VideoEncoderConfig{
				Encoding:   "H264",
				Resolution: onvifsrv.Resolution{Width: 1920, Height: 1080},
				Quality:    80,
				Framerate:  30,
				Bitrate:    4096,
				GovLength:  30,
			},
		})

		sdToken := fmt.Sprintf("profile_%s_sd", cam.DeviceID)
		profiles = append(profiles, onvifsrv.ProfileConfig{
			Token: sdToken,
			Name:  cam.DeviceName + " SD",
			VideoSource: onvifsrv.VideoSourceConfig{
				Token:      fmt.Sprintf("source_sd_%d", idx),
				Name:       cam.DeviceName + " SD Source",
				Resolution: onvifsrv.Resolution{Width: 1280, Height: 720},
				Framerate:  15,
			},
			VideoEncoder: onvifsrv.VideoEncoderConfig{
				Encoding:   "H264",
				Resolution: onvifsrv.Resolution{Width: 1280, Height: 720},
				Quality:    70,
				Framerate:  15,
				Bitrate:    2048,
				GovLength:  15,
			},
		})
	}

	config := &onvifsrv.Config{
		Host:     "0.0.0.0",
		Port:     s.port,
		BasePath: "/onvif",
		Timeout:  30 * time.Second,
		Username: "admin",
		Password: "admin",
		DeviceInfo: onvifsrv.DeviceInfo{
			Manufacturer:    "Tuya-IPC-Terminal",
			Model:           "ONVIF-Bridge",
			FirmwareVersion: "1.0.0",
			SerialNumber:    "TUYA-ONVIF-BRIDGE",
			HardwareID:      "TUYA-ONVIF",
		},
		Profiles:       profiles,
		SupportPTZ:     false,
		SupportImaging: false,
		SupportEvents:  false,
	}

	srv, err := onvifsrv.New(config)
	if err != nil {
		return fmt.Errorf("failed to create ONVIF SOAP context: %w", err)
	}

	for _, cam := range cameras {
		hdToken := fmt.Sprintf("profile_%s_hd", cam.DeviceID)
		sdToken := fmt.Sprintf("profile_%s_sd", cam.DeviceID)
		
		srv.UpdateStreamURI(hdToken, fmt.Sprintf("rtsp://%s:%d%s", localIP, s.rtspPort, cam.RTSPPath))
		srv.UpdateStreamURI(sdToken, fmt.Sprintf("rtsp://%s:%d%s/sd", localIP, s.rtspPort, cam.RTSPPath))
	}

	s.srvInstance = srv

	core.Logger.Info().Msgf("Starting ONVIF Host at http://0.0.0.0:%d/onvif/device_service", s.port)
	go func() {
		if err := srv.Start(ctx); err != nil {
			core.Logger.Error().Err(err).Msg("ONVIF Daemon closed")
		}
	}()

	return nil
}

func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "127.0.0.1"
	}
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return "127.0.0.1"
}
