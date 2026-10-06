package node

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSetCommitsWithOneFollowerDown(t *testing.T) {
	c := startRaftCluster(t, 3, 3)
	lead := waitLeader(t, c.nodes)

	var isolated *Node
	for _, n := range c.nodes {
		if n != lead {
			isolated = n
			break
		}
	}
	_ = isolated.Close()
	_ = c.srvs[isolated.ID()].Close()

	addr := c.addrs[lead.ID()]
	put, err := http.NewRequest(http.MethodPut, "http://"+addr+"/kv/user", strings.NewReader("alice"))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(put)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status=%d body=%s", resp.StatusCode, body)
	}
	if lead.CommitIndex() < 1 {
		t.Fatalf("leader commitIndex=%d", lead.CommitIndex())
	}

	got, err := http.Get("http://" + addr + "/kv/user")
	if err != nil {
		t.Fatal(err)
	}
	defer got.Body.Close()
	var kv map[string]any
	if err := json.NewDecoder(got.Body).Decode(&kv); err != nil {
		t.Fatal(err)
	}
	if kv["value"] != "alice" {
		t.Fatalf("got %#v", kv)
	}

	deadline := time.Now().Add(2 * time.Second)
	var caught *Node
	for time.Now().Before(deadline) {
		for _, n := range c.nodes {
			if n == lead || n == isolated {
				continue
			}
			if n.LastIndex() >= 1 && n.CommitIndex() >= 1 {
				caught = n
			}
		}
		if caught != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if caught == nil {
		t.Fatal("live follower did not catch up")
	}

	logb, err := os.ReadFile(filepath.Join(c.dirs[isolated.ID()], "log", "000001.jsonl"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if strings.Contains(string(logb), "alice") {
		t.Fatalf("isolated follower should lag, log=%s", logb)
	}
}
