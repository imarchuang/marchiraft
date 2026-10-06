package node

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func openTestNode(t *testing.T, dir, id string) *Node {
	t.Helper()
	n, err := Open(Config{ID: id, DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}

func TestPutGetHealthz(t *testing.T) {
	n := openTestNode(t, t.TempDir(), "n1")
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

	st, err := http.Get(srv.URL + "/raft/status")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Body.Close()
	sb, _ := io.ReadAll(st.Body)
	if st.StatusCode != http.StatusOK {
		t.Fatalf("status %d %s", st.StatusCode, sb)
	}
	if !strings.Contains(string(sb), `"logLength"`) {
		t.Fatalf("raft/status missing logLength: %s", sb)
	}
}

func TestGetMissing(t *testing.T) {
	n := openTestNode(t, t.TempDir(), "n1")
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

func TestRestartReplaysLogAndTerm(t *testing.T) {
	dir := t.TempDir()
	n1 := openTestNode(t, dir, "n1")
	if err := n1.appendAndApply("user", "alice"); err != nil {
		t.Fatal(err)
	}
	n1.mu.Lock()
	n1.term = 3
	if err := n1.persistMetaLocked(); err != nil {
		t.Fatal(err)
	}
	n1.mu.Unlock()
	if err := n1.Close(); err != nil {
		t.Fatal(err)
	}

	n2, err := Open(Config{ID: "n1", DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer n2.Close()
	n2.mu.Lock()
	defer n2.mu.Unlock()
	if n2.kv["user"] != "alice" {
		t.Fatalf("replay kv=%v", n2.kv)
	}
	if n2.term != 3 {
		t.Fatalf("term=%d want 3", n2.term)
	}
	if n2.commitIndex != 1 {
		t.Fatalf("commitIndex=%d", n2.commitIndex)
	}
	meta, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(meta), `"currentTerm": 3`) {
		t.Fatalf("meta.json: %s", meta)
	}
	logb, err := os.ReadFile(filepath.Join(dir, "log", "000001.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(logb), `"key":"user"`) {
		t.Fatalf("log: %s", logb)
	}
}
