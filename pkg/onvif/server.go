package onvif

import (
	"context"
	"fmt"
	"net"
	"regexp"
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
		cleanName := cleanCameraName(cam.DeviceName)

		hdToken := fmt.Sprintf("profile_%s_hd", cam.DeviceID)
		profiles = append(profiles, onvifsrv.ProfileConfig{
			Token: hdToken,
			Name:  cleanName + " HD",
			VideoSource: onvifsrv.VideoSourceConfig{
				Token:      fmt.Sprintf("source_%d", idx),
				Name:       cleanName + " Source",
				Resolution: onvifsrv.Resolution{Width: 1920, Height: 1080},
				Framerate:  20,
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
				Resolution: onvifsrv.Resolution{Width: 640, Height: 480},
				Framerate:  15,
			},
			VideoEncoder: onvifsrv.VideoEncoderConfig{
				Encoding:   "H264",
				Resolution: onvifsrv.Resolution{Width: 640, Height: 480},
				Quality:    70,
				Framerate:  15,
				Bitrate:    512,
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

	// Spawn our custom WS-Discovery responder on the network!
	go s.startWSDiscovery(ctx, localIP)

	return nil
}

func (s *ONVIFServer) startWSDiscovery(ctx context.Context, localIP string) {
	addr, err := net.ResolveUDPAddr("udp4", "239.255.255.250:3702")
	if err != nil {
		core.Logger.Error().Err(err).Msg("ONVIF Discovery: Failed to resolve WS-Discovery address")
		return
	}

	conn, err := net.ListenMulticastUDP("udp4", nil, addr)
	if err != nil {
		core.Logger.Error().Err(err).Msg("ONVIF Discovery: Failed to bind WS-Discovery multicast receiver")
		return
	}
	defer conn.Close()

	core.Logger.Info().Msg("ONVIF Discovery: Multicast WS-Discovery Listener is active on [239.255.255.250:3702]")

	buf := make([]byte, 8192)
	messageIDRegexp := regexp.MustCompile(`(?i)<[^:>]*:?MessageID[^>]*>uuid:([^<]+)</[^:>]*:?MessageID>`)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
			n, src, err := conn.ReadFromUDP(buf)
			if err != nil {
				continue
			}

			payload := string(buf[:n])
			// Match only WS-Discovery Probe targets
			if strings.Contains(payload, "Probe") && (strings.Contains(payload, "NetworkVideoTransmitter") || strings.Contains(payload, "Device")) {
				core.Logger.Info().Msgf("ONVIF Discovery: Received Probe from %s", src.String())

				// Extract MessageID to use as RelatesTo in reply
				match := messageIDRegexp.FindStringSubmatch(payload)
				messageUUID := "unspecified-uuid"
				if len(match) > 1 {
					messageUUID = match[1]
				}

				// Build correct SOAP payload pointing to our service
				replyPayload := buildProbeMatchesEnvelope(messageUUID, localIP, s.port)

				// Unicast reply back immediately to the sender's active IP and port!
				replyConn, err := net.DialUDP("udp4", nil, src)
				if err == nil {
					_, _ = replyConn.Write([]byte(replyPayload))
					_ = replyConn.Close()
					core.Logger.Info().Msgf("ONVIF Discovery: Replied ProbeMatches to %s (RelatesTo: %s)", src.String(), messageUUID)
				}
			}
		}
	}
}

func buildProbeMatchesEnvelope(messageUUID, localIP string, port int) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://www.w3.org/2003/05/soap-envelope" xmlns:wsa="http://schemas.xmlsoap.org/ws/2004/08/addressing" xmlns:wsd="http://schemas.xmlsoap.org/ws/2005/04/discovery" xmlns:dn="http://www.onvif.org/ver10/network/wsdl">
  <SOAP-ENV:Header>
    <wsa:Action>http://schemas.xmlsoap.org/ws/2005/04/discovery/ProbeMatches</wsa:Action>
    <wsa:MessageID>uuid:random-server-response-%d</wsa:MessageID>
    <wsa:RelatesTo>uuid:%s</wsa:RelatesTo>
    <wsa:To>http://schemas.xmlsoap.org/ws/2004/08/addressing/role/anonymous</wsa:To>
  </SOAP-ENV:Header>
  <SOAP-ENV:Body>
    <wsd:ProbeMatches>
      <wsd:ProbeMatch>
        <wsa:EndpointReference>
          <wsa:Address>urn:uuid:tuya-onvif-bridge-%s</wsa:Address>
        </wsa:EndpointReference>
        <wsd:Types>dn:NetworkVideoTransmitter</wsd:Types>
        <wsd:Scopes>onvif://www.onvif.org/type/NetworkVideoTransmitter onvif://www.onvif.org/name/TuyaBridge onvif://www.onvif.org/hardware/Gateway</wsd:Scopes>
        <wsd:XAddrs>http://%s:%d/onvif/device_service</wsd:XAddrs>
        <wsd:MetadataVersion>1</wsd:MetadataVersion>
      </wsd:ProbeMatch>
    </wsd:ProbeMatches>
  </SOAP-ENV:Body>
</SOAP-ENV:Envelope>`, time.Now().UnixNano(), messageUUID, localIP, localIP, port)
}

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
	
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ip := ipnet.IP.To4(); ip != nil {
				ipStr := ip.String()
				if strings.HasPrefix(ipStr, "192.168.") || (strings.HasPrefix(ipStr, "10.") && !strings.HasPrefix(ipStr, "10.255.")) {
					return ipStr
				}
			}
		}
	}
	
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ip := ipnet.IP.To4(); ip != nil {
				return ip.String()
			}
		}
	}
	return "127.0.0.1"
}
