package node

import (
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"
)

type raftCluster struct {
	nodes []*Node
	addrs map[string]string
	dirs  map[string]string
	srvs  map[string]*http.Server
}

func startCluster(t *testing.T, configured, live int) []*Node {
	t.Helper()
	return startRaftCluster(t, configured, live).nodes
}

func startRaftCluster(t *testing.T, configured, live int) *raftCluster {
	t.Helper()
	if live > configured {
		t.Fatalf("live=%d > configured=%d", live, configured)
	}
	ids := make([]string, configured)
	lns := make([]net.Listener, configured)
	peers := map[string]string{}
	for i := 0; i < configured; i++ {
		ids[i] = fmt.Sprintf("n%d", i+1)
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns[i] = ln
		peers[ids[i]] = ln.Addr().String()
		t.Cleanup(func() { _ = ln.Close() })
	}
	c := &raftCluster{
		addrs: copyStringMap(peers),
		dirs:  map[string]string{},
		srvs:  map[string]*http.Server{},
	}
	for i := 0; i < live; i++ {
		dir := t.TempDir()
		n, err := Open(Config{
			ID:                ids[i],
			DataDir:           dir,
			Peers:             copyStringMap(peers),
			ElectionTimeout:   40 * time.Millisecond,
			HeartbeatInterval: 15 * time.Millisecond,
			RPCTimeout:        50 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		srv := &http.Server{Handler: n.Handler()}
		go func(s *http.Server, ln net.Listener) { _ = s.Serve(ln) }(srv, lns[i])
		t.Cleanup(func() {
			_ = n.Close()
			_ = srv.Close()
		})
		n.Start()
		c.nodes = append(c.nodes, n)
		c.dirs[n.ID()] = dir
		c.srvs[n.ID()] = srv
	}
	return c
}

func waitLeader(t *testing.T, nodes []*Node) *Node {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var leaders []*Node
		for _, n := range nodes {
			if n.Role() == RoleLeader {
				leaders = append(leaders, n)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	var dump []string
	for _, n := range nodes {
		dump = append(dump, n.ID()+"="+n.Role())
	}
	t.Fatalf("expected exactly one leader, got %v", dump)
	return nil
}

func TestTwoOfThreeElectsLeader(t *testing.T) {
	nodes := startCluster(t, 3, 2)
	lead := waitLeader(t, nodes)
	t.Logf("leader=%s", lead.ID())
	others := 0
	for _, n := range nodes {
		if n != lead && n.Role() != RoleFollower && n.Role() != RoleCandidate {
			t.Fatalf("non-leader role=%s", n.Role())
		}
		if n != lead {
			others++
		}
	}
	if others != 1 {
		t.Fatalf("expected one other live node, got %d", others)
	}
}

func TestThreeElectsOneLeader(t *testing.T) {
	nodes := startCluster(t, 3, 3)
	waitLeader(t, nodes)
}
