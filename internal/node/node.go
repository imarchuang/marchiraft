// Package node is a single marchiraft process: HTTP KV plus Raft.
package node

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	RoleFollower  = "follower"
	RoleCandidate = "candidate"
	RoleLeader    = "leader"
)

// Config is process identity, listen address, peers, and durable data dir.
type Config struct {
	ID                string
	Listen            string
	DataDir           string
	Peers             map[string]string // id -> host:port
	ElectionTimeout   time.Duration     // min timeout; actual is [t, 2t)
	HeartbeatInterval time.Duration
	RPCTimeout        time.Duration
}

// Node is one replica.
type Node struct {
	cfg Config

	mu            sync.Mutex
	kv            map[string]string
	log           []LogEntry
	logFile       *os.File
	term          int
	votedFor      string
	role          string
	leaderID      string
	commitIndex   int
	lastApplied   int
	nextElection  time.Time
	nextHeartbeat time.Time
	nextIndex     map[string]int
	matchIndex    map[string]int
	stop          chan struct{}
	wg            sync.WaitGroup
	started       bool
	stopOnce      sync.Once
}

// Open creates or replays a node from dataDir (meta.json + log jsonl).
func Open(cfg Config) (*Node, error) {
	if cfg.ID == "" {
		cfg.ID = "n1"
	}
	if cfg.Peers == nil {
		cfg.Peers = map[string]string{}
	}
	n := &Node{
		cfg:  cfg,
		kv:   make(map[string]string),
		role: RoleFollower,
		stop: make(chan struct{}),
	}
	if err := n.openStore(); err != nil {
		return nil, err
	}
	n.mu.Lock()
	if n.voterCountLocked() == 1 {
		n.role = RoleLeader
		n.leaderID = n.cfg.ID
		n.initLeaderStateLocked()
	}
	n.mu.Unlock()
	return n, nil
}

func (n *Node) ID() string { return n.cfg.ID }

func (n *Node) Role() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

func (n *Node) LastIndex() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.log[len(n.log)-1].Index
}

func (n *Node) CommitIndex() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

func (n *Node) AddrOf(id string) string {
	return n.cfg.Peers[id]
}

func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", n.handleHealthz)
	mux.HandleFunc("/kv/", n.handleKV)
	mux.HandleFunc("/raft/status", n.handleStatus)
	mux.HandleFunc("/raft/vote", n.handleVote)
	mux.HandleFunc("/raft/append", n.handleAppend)
	return mux
}

func (n *Node) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	n.mu.Lock()
	defer n.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":          true,
		"id":          n.cfg.ID,
		"role":        n.role,
		"term":        n.term,
		"leader":      n.leaderID,
		"commitIndex": n.commitIndex,
	})
}

func (n *Node) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	last := n.log[len(n.log)-1]
	st := map[string]any{
		"id":          n.cfg.ID,
		"role":        n.role,
		"term":        n.term,
		"votedFor":    n.votedFor,
		"leader":      n.leaderID,
		"commitIndex": n.commitIndex,
		"lastApplied": n.lastApplied,
		"logLength":   last.Index,
		"lastLogTerm": last.Term,
		"peers":       n.cfg.Peers,
	}
	if n.role == RoleLeader {
		st["nextIndex"] = copyIntMap(n.nextIndex)
		st["matchIndex"] = copyIntMap(n.matchIndex)
	}
	writeJSON(w, http.StatusOK, st)
}

func (n *Node) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" || strings.Contains(key, "/") {
		http.Error(w, "key required", http.StatusBadRequest)
		return
	}
	n.mu.Lock()
	role := n.role
	leader := n.leaderID
	n.mu.Unlock()
	if role != RoleLeader {
		n.proxyToLeader(w, r, leader)
		return
	}
	switch r.Method {
	case http.MethodGet:
		n.getKV(w, key)
	case http.MethodPut:
		n.putKV(w, r, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (n *Node) getKV(w http.ResponseWriter, key string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	val, ok := n.kv[key]
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"key": key, "value": val})
}

func (n *Node) putKV(w http.ResponseWriter, r *http.Request, key string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := n.Propose(key, string(body)); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "key": key})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		fmt.Fprintf(w, `{"error":%q}`, err.Error())
	}
}
