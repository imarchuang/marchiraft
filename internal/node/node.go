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

func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", n.handleHealthz)
	mux.HandleFunc("/kv/", n.handleKV)
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

func (n *Node) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" || strings.Contains(key, "/") {
		http.Error(w, "key required", http.StatusBadRequest)
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
	n.mu.Lock()
	if n.role != RoleLeader {
		leader := n.leaderID
		n.mu.Unlock()
		http.Error(w, "not leader (leader="+leader+")", http.StatusServiceUnavailable)
		return
	}
	n.mu.Unlock()
	if err := n.appendAndApply(key, string(body)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
