package onvif

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strings"
	"syscall"
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
	overrideIP     string // Force a specific advertisement IP
}

func NewONVIFServer(port int, rtspPort int, sm *storage.StorageManager, overrideIP string) *ONVIFServer {
	return &ONVIFServer{
		port:           port,
		rtspPort:       rtspPort,
		storageManager: sm,
		overrideIP:     overrideIP,
	}
}

func (s *ONVIFServer) Start(ctx context.Context) error {
	cameras, err := s.storageManager.GetAllCameras()
	if err != nil {
		return fmt.Errorf("failed to load cameras for ONVIF server: %w", err)
	}

	var profiles []onvifsrv.ProfileConfig
	localIP := s.overrideIP
	if localIP == "" {
		localIP = getLocalIP()
	}
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
		Host:     "127.0.0.1", // Bind internally only so our proxy can sit on the external port!
		Port:     8081,        // Runs internally
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

	core.Logger.Info().Msgf("Starting Internal ONVIF Engine at http://127.0.0.1:8081/onvif/device_service")
	go func() {
		if err := srv.Start(ctx); err != nil {
			core.Logger.Error().Err(err).Msg("Internal ONVIF Engine closed")
		}
	}()

	// Spawn our custom SOAP Sanitizer/Logger Proxy on Port 80 (or s.port)
	go s.startSanitizingProxy(ctx, localIP)

	// Spawn our multi-interface WS-Discovery responder!
	go s.startWSDiscovery(ctx, localIP)

	return nil
}

func (s *ONVIFServer) startSanitizingProxy(ctx context.Context, localIP string) {
	proxyAddr := fmt.Sprintf("0.0.0.0:%d", s.port)
	core.Logger.Info().Msgf("Starting ONVIF Sanitizing Proxy on http://%s%s/device_service", proxyAddr, "/onvif")

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Read raw request body to log strict mismatches
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read request body", http.StatusInternalServerError)
			return
		}
		rawBody := string(bodyBytes)

		core.Logger.Info().Msgf("ONVIF Proxy: Received POST %s from %s", r.URL.Path, r.RemoteAddr)
		core.Logger.Debug().Msgf("ONVIF Proxy: Raw request headers: %v", r.Header)
		core.Logger.Debug().Msgf("ONVIF Proxy: Raw XML envelope:\n%s", rawBody)

		// NORMALIZE XML: Strip namespaces/prefixes to prevent Go's xml.Unmarshal from failing on legacy gSOAP structures!
		normalizedBody := normalizeSOAPEnvelope(rawBody)
		core.Logger.Debug().Msgf("ONVIF Proxy: Normalized XML envelope:\n%s", normalizedBody)

		// HANDLE empty-element requests that onvif-go can't process properly
		// These need direct responses to avoid 400 errors
		// Check raw body for GetSystemDateAndTime since normalization may not have completed
		if strings.Contains(rawBody, "GetSystemDateAndTime") && !strings.Contains(rawBody, "<Category>") {
			respBody := buildSystemDateAndTimeResponse()
			w.Header().Set("Content-Type", "application/soap+xml; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(respBody))
			return
		}

		// Create target request to internal ONVIF server
		targetURL := fmt.Sprintf("http://127.0.0.1:8081%s", r.URL.Path)
		req, err := http.NewRequest("POST", targetURL, strings.NewReader(normalizedBody))
		if err != nil {
			http.Error(w, "Failed to create forward request", http.StatusInternalServerError)
			return
		}

		// Clean up and forward headers
		for k, vv := range r.Header {
			for _, v := range vv {
				req.Header.Add(k, v)
			}
		}

		// Normalize Content-Type and SOAP action headers which legacy devices mismatch
		contentType := r.Header.Get("Content-Type")
		if contentType == "" {
			req.Header.Set("Content-Type", "application/soap+xml; charset=utf-8")
		} else if !strings.Contains(contentType, "charset") {
			req.Header.Set("Content-Type", contentType+"; charset=utf-8")
		}

		// Ensure SOAPAction is present if matching ver10 specs
		if r.Header.Get("SOAPAction") == "" && strings.Contains(rawBody, "GetSystemDateAndTime") {
			req.Header.Set("SOAPAction", "http://www.onvif.org/ver10/device/wsdl/GetSystemDateAndTime")
		}

		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			core.Logger.Error().Err(err).Msgf("ONVIF Proxy: Forward request failed")
			http.Error(w, "Engine unreachable", http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		respBody, _ := io.ReadAll(resp.Body)
		core.Logger.Info().Msgf("ONVIF Proxy: Forward responded %s with body size %d", resp.Status, len(respBody))

		// Copy response headers and body back to client
		for k, vv := range resp.Header {
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(respBody)
	})

	server := &http.Server{
		Addr:    proxyAddr,
		Handler: mux,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			core.Logger.Error().Err(err).Msg("ONVIF Sanitizing Proxy closed unexpectedly")
		}
	}()

	<-ctx.Done()
	_ = server.Shutdown(context.Background())
}

func (s *ONVIFServer) startWSDiscovery(ctx context.Context, localIP string) {
	gaddr, err := net.ResolveUDPAddr("udp4", "239.255.255.250:3702")
	if err != nil {
		core.Logger.Error().Err(err).Msg("ONVIF Discovery: Failed to resolve multicast group")
		return
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		core.Logger.Error().Err(err).Msg("ONVIF Discovery: Failed to list network interfaces")
		return
	}

	activeListeners := 0
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagMulticast == 0 {
			continue
		}

		conn, err := net.ListenMulticastUDP("udp4", &iface, gaddr)
		if err != nil {
			core.Logger.Debug().Msgf("ONVIF Discovery: Multicast bind ignored on interface %s: %v", iface.Name, err)
			continue
		}

		activeListeners++
		core.Logger.Info().Msgf("ONVIF Discovery: Multicast WS-Discovery Listener is active on interface [%s]", iface.Name)
		go s.listenOnConn(ctx, conn, iface.Name, localIP)
	}

	// Dynamic unicast listener directly on the advertised local IP
	uaddr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("%s:3702", localIP))
	if err == nil {
		// Use a custom ListenConfig to enforce socket reuse (SO_REUSEADDR) on Windows!
		// This allows our server to share port 3702 with the blocking Windows 'dashost'/DeviceAssociationService!
		lc := net.ListenConfig{
			Control: func(network, address string, c syscall.RawConn) error {
				_ = c.Control(func(fd uintptr) {
					setSocketReuseAddr(fd)
				})
				return nil
			},
		}
		unicastConn, err := lc.ListenPacket(ctx, "udp4", uaddr.String())
		if err == nil {
			activeListeners++
			core.Logger.Info().Msgf("ONVIF Discovery: Direct Unicast UDP Listener is active on [%s:3702] (Socket shared successfully!)", localIP)
			if udpConn, ok := unicastConn.(*net.UDPConn); ok {
				go s.listenOnConn(ctx, udpConn, "unicast-specific", localIP)
			}
		} else {
			core.Logger.Warn().Err(err).Msgf("ONVIF Discovery: Direct Unicast UDP Listener failed to bind to [%s:3702]", localIP)
		}
	}

	laddr, err := net.ResolveUDPAddr("udp4", "0.0.0.0:3702")
	if err == nil {
		// Use a custom ListenConfig to enforce socket reuse (SO_REUSEADDR) on Windows fallback!
		lc := net.ListenConfig{
			Control: func(network, address string, c syscall.RawConn) error {
				_ = c.Control(func(fd uintptr) {
					setSocketReuseAddr(fd)
				})
				return nil
			},
		}
		fallbackConn, err := lc.ListenPacket(ctx, "udp4", laddr.String())
		if err == nil {
			activeListeners++
			core.Logger.Info().Msg("ONVIF Discovery: Fallback Wildcard UDP Listener is active on [0.0.0.0:3702] (Socket shared successfully!)")
			if udpConn, ok := fallbackConn.(*net.UDPConn); ok {
				go s.listenOnConn(ctx, udpConn, "wildcard-fallback", localIP)
			}
		} else {
			core.Logger.Warn().Err(err).Msg("ONVIF Discovery: Fallback Wildcard UDP Listener failed to bind to [0.0.0.0:3702] (Port 3702 is likely occupied by Windows WSD/SSDP services)")
		}
	}

	if activeListeners == 0 {
		core.Logger.Error().Msg("ONVIF Discovery: CRITICAL! No active UDP interfaces could be bound on port 3702")
	}
}

func (s *ONVIFServer) listenOnConn(ctx context.Context, conn *net.UDPConn, ifaceName, localIP string) {
	defer conn.Close()
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

			// Detect both "Probe" and "Resolve" requests, and tolerate missing schema tags
			isProbe := strings.Contains(payload, "Probe") && (strings.Contains(payload, "NetworkVideoTransmitter") || strings.Contains(payload, "Device") || strings.Contains(payload, "scopes") || len(payload) < 2000)
			isResolve := strings.Contains(payload, "Resolve") && (strings.Contains(payload, "tuya-onvif-bridge") || strings.Contains(payload, "Device"))

			if isProbe || isResolve {
				core.Logger.Info().Msgf("ONVIF Discovery [%s]: Received Discovery Target (%s) from %s", ifaceName, map[bool]string{true: "Resolve", false: "Probe"}[isResolve], src.String())
				messageUUID := "unspecified-uuid"
				match := messageIDRegexp.FindStringSubmatch(payload)
				if len(match) > 1 {
					messageUUID = match[1]
				}

				var replyPayload string
				if isResolve {
					replyPayload = buildResolveMatchesEnvelope(messageUUID, localIP, s.port)
				} else {
					replyPayload = buildProbeMatchesEnvelope(messageUUID, localIP, s.port)
				}

				replyConn, err := net.DialUDP("udp4", nil, src)
				if err == nil {
					_, _ = replyConn.Write([]byte(replyPayload))
					_ = replyConn.Close()
					core.Logger.Info().Msgf("ONVIF Discovery [%s]: Replied Response to %s (RelatesTo: %s)", ifaceName, src.String(), messageUUID)
				} else {
					core.Logger.Error().Err(err).Msgf("ONVIF Discovery [%s]: Failed to unicast reply to %s", ifaceName, src.String())
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

func buildResolveMatchesEnvelope(messageUUID, localIP string, port int) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://www.w3.org/2003/05/soap-envelope" xmlns:wsa="http://schemas.xmlsoap.org/ws/2004/08/addressing" xmlns:wsd="http://schemas.xmlsoap.org/ws/2005/04/discovery" xmlns:dn="http://www.onvif.org/ver10/network/wsdl">
  <SOAP-ENV:Header>
    <wsa:Action>http://schemas.xmlsoap.org/ws/2005/04/discovery/ResolveMatches</wsa:Action>
    <wsa:MessageID>uuid:random-server-resolve-response-%d</wsa:MessageID>
    <wsa:RelatesTo>uuid:%s</wsa:RelatesTo>
    <wsa:To>http://schemas.xmlsoap.org/ws/2004/08/addressing/role/anonymous</wsa:To>
  </SOAP-ENV:Header>
  <SOAP-ENV:Body>
    <wsd:ResolveMatches>
      <wsd:ResolveMatch>
        <wsa:EndpointReference>
          <wsa:Address>urn:uuid:tuya-onvif-bridge-%s</wsa:Address>
        </wsa:EndpointReference>
        <wsd:Types>dn:NetworkVideoTransmitter</wsd:Types>
        <wsd:Scopes>onvif://www.onvif.org/type/NetworkVideoTransmitter onvif://www.onvif.org/name/TuyaBridge onvif://www.onvif.org/hardware/Gateway</wsd:Scopes>
        <wsd:XAddrs>http://%s:%d/onvif/device_service</wsd:XAddrs>
        <wsd:MetadataVersion>1</wsd:MetadataVersion>
      </wsd:ResolveMatch>
    </wsd:ResolveMatches>
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

// normalizeSOAPEnvelope strips problematic namespaces and normalizes empty tags
// to ensure compatibility with Go's strict xml.Unmarshal used by onvif-go
func normalizeSOAPEnvelope(xmlData string) string {
	// 1. Change all forms of SOAP namespaces so encoding/xml can parse it with standard Envelope tags
	replacements := []string{
		"<SOAP-ENV:Envelope", "<Envelope xmlns=\"http://www.w3.org/2003/05/soap-envelope\">",
		"</SOAP-ENV:Envelope>", "</Envelope>",
		"<SOAP-ENV:Header", "<Header",
		"</SOAP-ENV:Header>", "</Header>",
		"<SOAP-ENV:Body", "<Body",
		"</SOAP-ENV:Body>", "</Body>",

		"<s:Envelope", "<Envelope xmlns=\"http://www.w3.org/2003/05/soap-envelope\">",
		"</s:Envelope>", "</Envelope>",
		"<s:Header", "<Header",
		"</s:Header>", "</Header>",
		"<s:Body", "<Body",
		"</s:Body>", "</Body>",
	}
	r := strings.NewReplacer(replacements...)
	xmlData = r.Replace(xmlData)

	// Clean nested namespace references inside SOAP tags so they decode natively
	xmlData = strings.ReplaceAll(xmlData, "SOAP-ENV:", "")
	xmlData = strings.ReplaceAll(xmlData, "SOAP-ENC:", "")
	xmlData = strings.ReplaceAll(xmlData, "wsse:", "")
	xmlData = strings.ReplaceAll(xmlData, "wsu:", "")

	// 2. Strip xmlns declarations on body action tags that cause Go's xml.Unmarshal to fail
	// Pattern: <GetSystemDateAndTime xmlns="..."> or <GetCapabilities xmlns="..." ...>
	xmlnsPattern := regexp.MustCompile(` xmlns="http://www\.onvif\.org/ver10/(?:device|media|network)/wsdl"[^>]*>`)
	xmlData = xmlnsPattern.ReplaceAllString(xmlData, ">")

	// 3. Collapse empty tags to self-closing style (Go xml.Unmarshal prefers this)
	// e.g., <GetSystemDateAndTime></GetSystemDateAndTime> -> <GetSystemDateAndTime/>
	xmlData = regexp.MustCompile(`<([A-Za-z]+)></\1>`).ReplaceAllString(xmlData, "<$1/>")

	return xmlData
}

// buildSystemDateAndTimeResponse returns a valid ONVIF GetSystemDateAndTime response
func buildSystemDateAndTimeResponse() string {
	now := time.Now().UTC()
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<Envelope xmlns="http://www.w3.org/2003/05/soap-envelope">
  <Header/>
  <Body>
    <GetSystemDateAndTimeResponse xmlns="http://www.onvif.org/ver10/device/wsdl">
      <SystemDateAndTime>
        <DateTimeType>DateTime</DateTimeType>
        <UtcDateTime>
          <Date><Year>%d</Year><Month>%d</Month><Day>%d</Day></Date>
          <Time><Hour>%d</Hour><Minute>%d</Minute><Second>%d</Second></Time>
        </UtcDateTime>
        <LocalDateTime>
          <Date><Year>%d</Year><Month>%d</Month><Day>%d</Day></Date>
          <Time><Hour>%d</Hour><Minute>%d</Minute><Second>%d</Second></Time>
        </LocalDateTime>
        <DaylightSavings>false</DaylightSavings>
        <DaylightSavingsOffset>PT0H</DaylightSavingsOffset>
      </SystemDateAndTime>
    </GetSystemDateAndTimeResponse>
  </Body>
</Envelope>`,
		now.Year(), int(now.Month()), now.Day(), now.Hour(), now.Minute(), now.Second(),
		now.Year(), int(now.Month()), now.Day(), now.Hour(), now.Minute(), now.Second())
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
				ipStr := ip.String()
				if strings.HasPrefix(ipStr, "192.168.") && !strings.HasPrefix(ipStr, "192.168.56.") {
					return ipStr
				}
			}
		}
	}
	return "127.0.0.1"
}