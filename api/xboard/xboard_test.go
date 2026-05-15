package xboard

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/XrayR-project/XrayR/api"
	"github.com/gorilla/websocket"
)

func TestXboardClientNodeUsersAndReport(t *testing.T) {
	var reportPayload map[string]interface{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v2/server/handshake":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"websocket": map[string]interface{}{"enabled": false},
				"settings":  map[string]interface{}{"push_interval": 15, "pull_interval": 30},
			})
		case "/api/v1/server/UniProxy/config":
			if got := r.URL.Query().Get("node_id"); got != "7" {
				t.Fatalf("node_id query = %q", got)
			}
			if got := r.URL.Query().Get("node_type"); got != "vless" {
				t.Fatalf("node_type query = %q", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"protocol":    "vless",
				"server_port": 443,
				"network":     "ws",
				"networkSettings": map[string]interface{}{
					"path":    "/ray",
					"headers": map[string]interface{}{"Host": "node.example.com"},
				},
				"tls":  2,
				"flow": "xtls-rprx-vision",
				"tls_settings": map[string]interface{}{
					"server_name": "www.example.com",
					"server_port": "443",
					"dest":        "www.example.com",
					"private_key": "private",
					"short_id":    "abcd",
				},
				"routes": []map[string]interface{}{
					{"id": 9, "match": []string{"example.org"}, "action": "block"},
				},
			})
		case "/api/v1/server/UniProxy/user":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"users": []map[string]interface{}{
					{"id": 1, "uuid": "uuid-1", "speed_limit": 8, "device_limit": 2},
				},
			})
		case "/api/v2/server/report":
			if err := json.NewDecoder(r.Body).Decode(&reportPayload); err != nil {
				t.Fatal(err)
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"ok": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := New(&api.Config{
		APIHost:     server.URL,
		NodeID:      7,
		Key:         "token",
		NodeType:    "Vless",
		EnableVless: true,
		Timeout:     1,
	})

	node, err := client.GetNodeInfo()
	if err != nil {
		t.Fatal(err)
	}
	if node.NodeType != "Vless" || node.Port != 443 || node.TransportProtocol != "ws" {
		t.Fatalf("unexpected node: %#v", node)
	}
	if !node.EnableREALITY || node.REALITYConfig.PrivateKey != "private" {
		t.Fatalf("reality config not parsed: %#v", node.REALITYConfig)
	}
	if node.Host != "node.example.com" || node.Path != "/ray" {
		t.Fatalf("network settings not parsed: host=%q path=%q", node.Host, node.Path)
	}

	users, err := client.GetUserList()
	if err != nil {
		t.Fatal(err)
	}
	if len(*users) != 1 || (*users)[0].DeviceLimit != 2 || (*users)[0].SpeedLimit != 1000000 {
		t.Fatalf("unexpected users: %#v", users)
	}

	rules, err := client.GetNodeRule()
	if err != nil {
		t.Fatal(err)
	}
	if len(*rules) != 1 || (*rules)[0].ID != 9 {
		t.Fatalf("unexpected rules: %#v", rules)
	}

	err = client.ReportUserTraffic(&[]api.UserTraffic{{UID: 1, Upload: 11, Download: 22}})
	if err != nil {
		t.Fatal(err)
	}
	if reportPayload["token"] != "token" || reportPayload["node_id"].(float64) != 7 {
		t.Fatalf("auth not injected: %#v", reportPayload)
	}
	if _, ok := reportPayload["traffic"]; !ok {
		t.Fatalf("traffic missing from report: %#v", reportPayload)
	}
}

func TestXboardClientDecodesWeakTypedPayloads(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v2/server/handshake":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"websocket": map[string]interface{}{"enabled": false},
			})
		case "/api/v1/server/UniProxy/config":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"protocol":    "vless",
				"server_port": "8443",
				"network":     "ws",
				"tls":         "2",
				"networkSettings": map[string]interface{}{
					"path": "/weak",
					"headers": map[string]interface{}{
						"Host": "weak.example.com",
					},
				},
				"tls_settings": map[string]interface{}{
					"server_name": "www.example.com",
					"server_port": "443",
					"dest":        "www.example.com",
					"private_key": "private",
					"short_ids":   []string{"abcd"},
				},
			})
		case "/api/v1/server/UniProxy/user":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"users": []map[string]interface{}{
					{"id": "1", "uuid": "uuid-1", "speed_limit": "8", "device_limit": "2"},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := New(&api.Config{
		APIHost:     server.URL,
		NodeID:      7,
		Key:         "token",
		NodeType:    "Vless",
		EnableVless: true,
		Timeout:     1,
	})

	node, err := client.GetNodeInfo()
	if err != nil {
		t.Fatal(err)
	}
	if node.Port != 8443 || !node.EnableREALITY || node.Path != "/weak" || node.Host != "weak.example.com" {
		t.Fatalf("weak typed node payload not decoded: %#v", node)
	}

	users, err := client.GetUserList()
	if err != nil {
		t.Fatal(err)
	}
	if len(*users) != 1 || (*users)[0].UID != 1 || (*users)[0].DeviceLimit != 2 || (*users)[0].SpeedLimit != 1000000 {
		t.Fatalf("weak typed users payload not decoded: %#v", users)
	}
}

func TestXboardClientAppliesWeakTypedWebSocketUserDelta(t *testing.T) {
	client := New(&api.Config{APIHost: "http://127.0.0.1", NodeID: 7, Key: "token", NodeType: "Vless"})
	client.setUsers([]user{{ID: 1, UUID: "uuid-1", SpeedLimit: 8, DeviceLimit: 2}})
	client.sentUsers = client.userVer

	client.handleWSMessage(wsMessage{
		Event: wsEventSyncUserDelta,
		Data:  json.RawMessage(`{"action":"add","users":[{"id":"2","uuid":"uuid-2","speed_limit":"16","device_limit":"3"}]}`),
	}, nil)

	users, ok := client.cachedUserList()
	if !ok {
		t.Fatal("user delta did not mark cached user list as changed")
	}
	byID := make(map[int]api.UserInfo)
	for _, item := range *users {
		byID[item.UID] = item
	}
	if byID[2].UUID != "uuid-2" || byID[2].DeviceLimit != 3 || byID[2].SpeedLimit != 2000000 {
		t.Fatalf("weak typed delta user not applied: %#v", users)
	}
}

func TestXboardClientReportsEmptyDevicesOverWebSocket(t *testing.T) {
	client := New(&api.Config{APIHost: "http://127.0.0.1", NodeID: 7, Key: "token", NodeType: "Vless"})
	writeCh := make(chan wsMessage, 1)
	client.wsWriteCh = writeCh

	err := client.ReportNodeOnlineUsers(&[]api.OnlineUser{})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-writeCh:
		if msg.Event != wsEventReportDevices {
			t.Fatalf("unexpected event: %s", msg.Event)
		}
		var devices map[string][]string
		if err := json.Unmarshal(msg.Data, &devices); err != nil {
			t.Fatal(err)
		}
		if len(devices) != 0 {
			t.Fatalf("expected empty devices payload, got %s", msg.Data)
		}
	case <-time.After(time.Second):
		t.Fatal("empty device report was not sent over websocket")
	}
}

func TestXboardClientReportsDevicesOverWebSocket(t *testing.T) {
	deviceReport := make(chan json.RawMessage, 1)
	connected := make(chan struct{})
	upgrader := websocket.Upgrader{}
	wsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("token"); got != "token" {
			t.Fatalf("ws token = %q", got)
		}
		if got := r.URL.Query().Get("node_id"); got != "7" {
			t.Fatalf("ws node_id = %q", got)
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if err := conn.WriteJSON(wsMessage{Event: "auth.success"}); err != nil {
			t.Fatal(err)
		}
		close(connected)
		for {
			var msg wsMessage
			if err := conn.ReadJSON(&msg); err != nil {
				return
			}
			if msg.Event == wsEventReportDevices {
				deviceReport <- msg.Data
				return
			}
		}
	}))
	defer wsServer.Close()
	wsURL := "ws" + strings.TrimPrefix(wsServer.URL, "http")

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v2/server/handshake":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"websocket": map[string]interface{}{"enabled": true, "ws_url": wsURL},
				"settings":  map[string]interface{}{"push_interval": 15, "pull_interval": 30},
			})
		case "/api/v1/server/UniProxy/config":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"protocol":    "vless",
				"server_port": 443,
				"network":     "tcp",
			})
		case "/api/v2/server/report":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{"ok": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer apiServer.Close()

	client := New(&api.Config{
		APIHost:     apiServer.URL,
		NodeID:      7,
		Key:         "token",
		NodeType:    "Vless",
		EnableVless: true,
		Timeout:     1,
	})
	defer client.Close()

	if _, err := client.GetNodeInfo(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-connected:
	case <-time.After(2 * time.Second):
		t.Fatal("websocket did not connect")
	}
	err := client.ReportNodeOnlineUsers(&[]api.OnlineUser{{UID: 1, IP: "1.1.1.1"}})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case raw := <-deviceReport:
		var devices map[string][]string
		if err := json.Unmarshal(raw, &devices); err != nil {
			t.Fatal(err)
		}
		if len(devices["1"]) != 1 || devices["1"][0] != "1.1.1.1" {
			t.Fatalf("unexpected devices payload: %s", raw)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("device report was not sent over websocket")
	}
}
