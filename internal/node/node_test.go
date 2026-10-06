package node

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPutGetHealthz(t *testing.T) {
	n := New(Config{ID: "n1"})
	srv := httptest.NewServer(n.Handler())
	t.Cleanup(srv.Close)

	put, err := http.NewRequest(http.MethodPut, srv.URL+"/kv/user", strings.NewReader("alice"))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(put)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status=%d", resp.StatusCode)
	}

	got, err := http.Get(srv.URL + "/kv/user")
	if err != nil {
		t.Fatal(err)
	}
	defer got.Body.Close()
	if got.StatusCode != http.StatusOK {
		t.Fatalf("GET status=%d", got.StatusCode)
	}
	var kv map[string]any
	if err := json.NewDecoder(got.Body).Decode(&kv); err != nil {
		t.Fatal(err)
	}
	if kv["value"] != "alice" {
		t.Fatalf("want alice, got %#v", kv)
	}

	hz, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer hz.Body.Close()
	body, _ := io.ReadAll(hz.Body)
	if hz.StatusCode != http.StatusOK {
		t.Fatalf("healthz %d %s", hz.StatusCode, body)
	}
	if !strings.Contains(string(body), `"role"`) {
		t.Fatalf("healthz missing role: %s", body)
	}
}

func TestGetMissing(t *testing.T) {
	n := New(Config{ID: "n1"})
	srv := httptest.NewServer(n.Handler())
	t.Cleanup(srv.Close)
	resp, err := http.Get(srv.URL + "/kv/missing")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d", resp.StatusCode)
	}
}
