package onvif

import (
	"context"
	"fmt"
	"net"
	"strings"
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
	core.Logger.Info().Msgf("ONVIF Local Network advertising address chosen: %s", localIP)

	for idx, cam := range cameras {
		// Clean camera name to avoid Polish letters and spaces which crash hardware screens!
		cleanName := cleanCameraName(cam.DeviceName)

		hdToken := fmt.Sprintf("profile_%s_hd", cam.DeviceID)
		profiles = append(profiles, onvifsrv.ProfileConfig{
			Token: hdToken,
			Name:  cleanName + " HD",
			VideoSource: onvifsrv.VideoSourceConfig{
				Token:      fmt.Sprintf("source_%d", idx),
				Name:       cleanName + " Source",
				Resolution: onvifsrv.Resolution{Width: 1920, Height: 1080},
				Framerate:  20, // Lower framerate helper for intercom stability
			},
			VideoEncoder: onvifsrv.VideoEncoderConfig{
				Encoding:   "H264",
				Resolution: onvifsrv.Resolution{Width: 1920, Height: 1080},
				Quality:    80,
				Framerate:  20,
				Bitrate:    2048,
				GovLength:  20,
			},
		})

		sdToken := fmt.Sprintf("profile_%s_sd", cam.DeviceID)
		profiles = append(profiles, onvifsrv.ProfileConfig{
			Token: sdToken,
			Name:  cleanName + " SD",
			VideoSource: onvifsrv.VideoSourceConfig{
				Token:      fmt.Sprintf("source_sd_%d", idx),
				Name:       cleanName + " SD Source",
				Resolution: onvifsrv.Resolution{Width: 640, Height: 480}, // VGA is highly compatible with Force Piano!
				Framerate:  15,
			},
			VideoEncoder: onvifsrv.VideoEncoderConfig{
				Encoding:   "H264",
				Resolution: onvifsrv.Resolution{Width: 640, Height: 480},
				Quality:    70,
				Framerate:  15,
				Bitrate:    512, // Minimal footprint
				GovLength:  15,
			},
		})
	}

	config := &onvifsrv.Config{
		Host:     "0.0.0.0",
		Port:     s.port,
		BasePath: "/onvif",
		Timeout:  30 * time.Second,
		Username: "admin", // Standard credentials manually requested on add IP camera forms!
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

// Replaces spaces and Polish characters which cause wideodomofony parsing to drop or fail
func cleanCameraName(name string) string {
	r := strings.NewReplacer(
		" ", "_",
		"ą", "a", "ć", "c", "ę", "e", "ł", "l", "ń", "n", "ó", "o", "ś", "s", "ź", "z", "ż", "z",
		"Ą", "A", "Ć", "C", "Ę", "E", "Ł", "L", "Ń", "N", "Ó", "O", "Ś", "S", "Ź", "Z", "Ż", "Z",
	)
	return r.Replace(name)
}

func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "127.0.0.1"
	}
	
	// Prioritize traditional local home network IPs (192.168.x.x, 10.x.x.x but not docker/virtual bridge ranges)
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ip := ipnet.IP.To4(); ip != nil {
				ipStr := ip.String()
				// Exclude Docker virtual networks, local ranges of virtual machines (like 172.17.x.x etc.)
				if strings.HasPrefix(ipStr, "192.168.") || (strings.HasPrefix(ipStr, "10.") && !strings.HasPrefix(ipStr, "10.255.")) {
					return ipStr
				}
			}
		}
	}
	
	// Fallback to any real non-loopback IPv4 address
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ip := ipnet.IP.To4(); ip != nil {
				return ip.String()
			}
		}
	}
	return "127.0.0.1"
}
