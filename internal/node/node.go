// Package node is a single marchiraft process: HTTP KV plus (later) Raft.
package node

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
)

const (
	RoleFollower = "follower"
	RoleLeader   = "leader"
)

// Config is process identity, listen address, and durable data dir.
type Config struct {
	ID      string
	Listen  string
	DataDir string
	Peers   map[string]string // id -> host:port
}

// Node is one replica. Slice 1: local log + term, replay on open.
type Node struct {
	cfg Config

	mu          sync.Mutex
	kv          map[string]string
	log         []LogEntry
	logFile     *os.File
	term        int
	votedFor    string
	role        string
	commitIndex int
	lastApplied int
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
		role: RoleLeader, // single node; clustering comes later
	}
	if err := n.openStore(); err != nil {
		return nil, err
	}
	return n, nil
}

func (n *Node) ID() string { return n.cfg.ID }

func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", n.handleHealthz)
	mux.HandleFunc("/kv/", n.handleKV)
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
