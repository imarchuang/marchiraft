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

func TestFailoverCommittedKeysSurvive(t *testing.T) {
	c := startRaftCluster(t, 3, 3)
	old := waitLeader(t, c.nodes)
	resp := httpPut(t, c.addrs[old.ID()], "user", "alice")
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT %d", resp.StatusCode)
	}

	oldID := old.ID()
	c.stopID(oldID)
	newLead := waitLeader(t, c.nodes)
	if newLead.ID() == oldID {
		t.Fatal("old leader still leading")
	}

	// GET via a follower should proxy to the new leader.
	var follower *Node
	for _, n := range c.nodes {
		if n != newLead {
			follower = n
			break
		}
	}
	deadline := time.Now().Add(3 * time.Second)
	var kv map[string]any
	for time.Now().Before(deadline) {
		got, err := http.Get("http://" + c.addrs[follower.ID()] + "/kv/user")
		if err != nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		body, _ := io.ReadAll(got.Body)
		got.Body.Close()
		if got.StatusCode == http.StatusOK {
			if err := json.Unmarshal(body, &kv); err == nil && kv["value"] == "alice" {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("committed key missing after failover, last=%#v", kv)
}

func TestUncommittedTailOverwritten(t *testing.T) {
	c := startRaftCluster(t, 3, 3)
	old := waitLeader(t, c.nodes)
	resp := httpPut(t, c.addrs[old.ID()], "user", "alice")
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT alice %d", resp.StatusCode)
	}

	var followers []string
	for _, n := range c.nodes {
		if n.ID() != old.ID() {
			followers = append(followers, n.ID())
		}
	}
	for _, id := range followers {
		c.stopID(id)
	}

	resp = httpPut(t, c.addrs[old.ID()], "user", "bob")
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Fatalf("uncommitted PUT should not succeed, body=%s", b)
	}
	if old.LastIndex() <= old.CommitIndex() {
		t.Fatalf("want uncommitted tail, last=%d commit=%d", old.LastIndex(), old.CommitIndex())
	}
	oldLog, err := os.ReadFile(filepath.Join(c.dirs[old.ID()], "log", "000001.jsonl"))
	if err != nil || !strings.Contains(string(oldLog), `"value":"bob"`) {
		t.Fatalf("old leader should have uncommitted bob: %s", oldLog)
	}
	oldID := old.ID()
	c.stopID(oldID)

	for _, id := range followers {
		c.restartID(t, id)
	}
	newLead := waitLeader(t, c.nodes)

	got, err := http.Get("http://" + c.addrs[newLead.ID()] + "/kv/user")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(got.Body)
	got.Body.Close()
	if got.StatusCode != http.StatusOK || !strings.Contains(string(body), "alice") {
		t.Fatalf("expected alice after failover: %d %s", got.StatusCode, body)
	}

	c.restartID(t, oldID)
	resp = httpPut(t, c.addrs[newLead.ID()], "extra", "1")
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT extra %d", resp.StatusCode)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		logb, err := os.ReadFile(filepath.Join(c.dirs[oldID], "log", "000001.jsonl"))
		if err == nil && strings.Contains(string(logb), `"value":"alice"`) && !strings.Contains(string(logb), `"value":"bob"`) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	logb, _ := os.ReadFile(filepath.Join(c.dirs[oldID], "log", "000001.jsonl"))
	t.Fatalf("old leader log should drop uncommitted bob: %s", logb)
}
