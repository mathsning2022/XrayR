package xboard

import (
	"encoding/json"
	"fmt"
	"strings"
)

type handshakeResponse struct {
	WebSocket wsConfig `json:"websocket"`
	Settings  settings `json:"settings"`
}

func (h *handshakeResponse) UnmarshalJSON(data []byte) error {
	type alias handshakeResponse
	aux := &struct {
		Settings json.RawMessage `json:"settings"`
		*alias
	}{alias: (*alias)(h)}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if len(aux.Settings) > 0 && string(aux.Settings) != "[]" {
		if err := json.Unmarshal(aux.Settings, &h.Settings); err != nil {
			return err
		}
	}
	return nil
}

type wsConfig struct {
	Enabled bool   `json:"enabled"`
	WSURL   string `json:"ws_url,omitempty"`
}

type settings struct {
	PushInterval int `json:"push_interval"`
	PullInterval int `json:"pull_interval"`
}

type stringOrArray string

func (s *stringOrArray) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		*s = stringOrArray(str)
		return nil
	}
	var arr []string
	if err := json.Unmarshal(data, &arr); err == nil {
		*s = stringOrArray(strings.Join(arr, "\n"))
		return nil
	}
	return fmt.Errorf("expected string or []string, got %s", string(data))
}

type nodeConfig struct {
	NodeID          int                    `json:"node_id,omitempty"`
	Protocol        string                 `json:"protocol"`
	ListenIP        string                 `json:"listen_ip"`
	ServerPort      int                    `json:"server_port"`
	Network         string                 `json:"network"`
	NetworkSettings map[string]interface{} `json:"networkSettings"`
	BaseConfig      settings               `json:"base_config"`
	Routes          []route                `json:"routes"`

	KernelType       string            `json:"kernel_type,omitempty"`
	KernelLogLevel   string            `json:"kernel_log_level,omitempty"`
	CustomOutbounds  []outboundConfig  `json:"custom_outbounds,omitempty"`
	CustomRoutes     []map[string]any  `json:"custom_routes,omitempty"`
	CustomRouteRules []customRouteRule `json:"custom_route_rules,omitempty"`
	CertConfig       *certConfig       `json:"cert_config,omitempty"`
	AutoTLS          bool              `json:"auto_tls,omitempty"`
	Domain           string            `json:"domain,omitempty"`
	Multiplex        *multiplexConfig  `json:"multiplex,omitempty"`
	PaddingScheme    stringOrArray     `json:"padding_scheme,omitempty"`
	TLSSettings      map[string]any    `json:"tls_settings,omitempty"`
	AcceptProxy      bool              `json:"accept_proxy_protocol,omitempty"`
	Cipher           string            `json:"cipher,omitempty"`
	Plugin           string            `json:"plugin,omitempty"`
	PluginOpt        string            `json:"plugin_opts,omitempty"`
	ServerKey        string            `json:"server_key,omitempty"`
	TLS              int               `json:"tls,omitempty"`
	Flow             string            `json:"flow,omitempty"`
	Decryption       string            `json:"decryption,omitempty"`
	Host             string            `json:"host,omitempty"`
	ServerName       string            `json:"server_name,omitempty"`
	Version          int               `json:"version,omitempty"`
	UpMbps           int               `json:"up_mbps,omitempty"`
	DownMbps         int               `json:"down_mbps,omitempty"`
	Obfs             string            `json:"obfs,omitempty"`
	ObfsPassword     string            `json:"obfs-password,omitempty"`
	Congestion       string            `json:"congestion_control,omitempty"`
	Transport        string            `json:"transport,omitempty"`
	TrafficPattern   string            `json:"traffic_pattern,omitempty"`
}

type certConfig struct {
	CertMode    string            `json:"cert_mode"`
	Domain      string            `json:"domain"`
	Email       string            `json:"email"`
	DNSProvider string            `json:"dns_provider"`
	DNSEnv      map[string]string `json:"dns_env"`
	HTTPPort    int               `json:"http_port"`
	CertFile    string            `json:"cert_file"`
	KeyFile     string            `json:"key_file"`
	CertContent string            `json:"cert_content"`
	KeyContent  string            `json:"key_content"`
}

type outboundConfig struct {
	Tag      string         `json:"tag"`
	Protocol string         `json:"protocol"`
	Settings map[string]any `json:"settings,omitempty"`
	ProxyTag string         `json:"proxy_tag,omitempty"`
}

type multiplexConfig struct {
	Enabled        bool          `json:"enabled"`
	Protocol       string        `json:"protocol"`
	MaxConnections int           `json:"max_connections"`
	MinStreams     int           `json:"min_streams"`
	MaxStreams     int           `json:"max_streams"`
	Padding        bool          `json:"padding"`
	Brutal         *brutalConfig `json:"brutal,omitempty"`
}

type brutalConfig struct {
	Enabled  bool `json:"enabled"`
	UpMbps   int  `json:"up_mbps"`
	DownMbps int  `json:"down_mbps"`
}

type customRouteRule struct {
	Name     string      `json:"name,omitempty"`
	Disabled bool        `json:"disabled,omitempty"`
	Match    routeMatch  `json:"match,omitempty"`
	Action   routeAction `json:"action"`
}

type routeMatch struct {
	Domains        []string `json:"domains,omitempty"`
	DomainSuffixes []string `json:"domain_suffixes,omitempty"`
	IPCIDRs        []string `json:"ip_cidrs,omitempty"`
	Ports          []string `json:"ports,omitempty"`
	Networks       []string `json:"networks,omitempty"`
	SourceCIDRs    []string `json:"source_cidrs,omitempty"`
	SourcePorts    []string `json:"source_ports,omitempty"`
}

type routeAction struct {
	Type   string `json:"type"`
	Target string `json:"target,omitempty"`
}

type route struct {
	ID          int      `json:"id"`
	Match       []string `json:"match"`
	Action      string   `json:"action"`
	ActionValue string   `json:"action_value,omitempty"`
}

type user struct {
	ID          int    `json:"id"`
	UUID        string `json:"uuid"`
	SpeedLimit  int    `json:"speed_limit"`
	DeviceLimit int    `json:"device_limit"`
}

type usersResponse struct {
	Users []user `json:"users"`
}
