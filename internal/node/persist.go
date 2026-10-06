package node

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const (
	cmdSet     = "SET"
	cmdNoop    = "NOOP"
	logFileRel = "log/000001.jsonl"
	metaRel    = "meta.json"
)

// LogEntry is one replicated command. Index is 1-based.
type LogEntry struct {
	Index int    `json:"index"`
	Term  int    `json:"term"`
	Type  string `json:"type"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

type metaFile struct {
	CurrentTerm int    `json:"currentTerm"`
	VotedFor    string `json:"votedFor"`
	NodeID      string `json:"nodeID"`
}

func (n *Node) openStore() error {
	if n.cfg.DataDir == "" {
		return fmt.Errorf("dataDir required")
	}
	if err := os.MkdirAll(filepath.Join(n.cfg.DataDir, "log"), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(n.cfg.DataDir, "snapshot"), 0o755); err != nil {
		return err
	}
	if err := n.loadMeta(); err != nil {
		return err
	}
	if err := n.replayLog(); err != nil {
		return err
	}
	f, err := os.OpenFile(n.logPath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	n.logFile = f
	return nil
}

func (n *Node) Close() error {
	n.stopOnce.Do(func() {
		close(n.stop)
	})
	n.wg.Wait()
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.logFile != nil {
		err := n.logFile.Close()
		n.logFile = nil
		return err
	}
	return nil
}

func (n *Node) logPath() string {
	return filepath.Join(n.cfg.DataDir, logFileRel)
}

func (n *Node) metaPath() string {
	return filepath.Join(n.cfg.DataDir, metaRel)
}

func (n *Node) loadMeta() error {
	b, err := os.ReadFile(n.metaPath())
	if os.IsNotExist(err) {
		n.term = 0
		n.votedFor = ""
		return n.persistMetaLocked()
	}
	if err != nil {
		return err
	}
	var m metaFile
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	n.term = m.CurrentTerm
	n.votedFor = m.VotedFor
	return nil
}

func (n *Node) persistMetaLocked() error {
	return atomicWriteJSON(n.metaPath(), metaFile{
		CurrentTerm: n.term,
		VotedFor:    n.votedFor,
		NodeID:      n.cfg.ID,
	})
}

func (n *Node) replayLog() error {
	n.log = []LogEntry{{Index: 0, Term: 0}} // dummy
	n.kv = make(map[string]string)
	f, err := os.Open(n.logPath())
	if os.IsNotExist(err) {
		n.commitIndex = 0
		n.lastApplied = 0
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var e LogEntry
		if err := json.Unmarshal(line, &e); err != nil {
			return fmt.Errorf("log replay: %w", err)
		}
		n.log = append(n.log, e)
		if e.Type == cmdSet {
			n.kv[e.Key] = e.Value
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	last := n.log[len(n.log)-1].Index
	n.commitIndex = last
	n.lastApplied = last
	return nil
}

func (n *Node) appendAndApply(key, value string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	e, err := n.appendLocked(key, value)
	if err != nil {
		return err
	}
	n.commitIndex = e.Index
	n.applyCommittedLocked()
	return nil
}

func (n *Node) appendLocked(key, value string) (LogEntry, error) {
	idx := n.log[len(n.log)-1].Index + 1
	e := LogEntry{Index: idx, Term: n.term, Type: cmdSet, Key: key, Value: value}
	if err := n.fsyncAppendLocked(e); err != nil {
		return LogEntry{}, err
	}
	n.log = append(n.log, e)
	return e, nil
}

func (n *Node) applyCommittedLocked() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		e := n.log[n.lastApplied]
		if e.Type == cmdSet {
			n.kv[e.Key] = e.Value
		}
	}
}

func (n *Node) truncateFromLocked(index int) error {
	if index < 1 {
		index = 1
	}
	if index >= len(n.log) {
		return nil
	}
	n.log = n.log[:index]
	if n.commitIndex > n.log[len(n.log)-1].Index {
		n.commitIndex = n.log[len(n.log)-1].Index
	}
	if n.lastApplied > n.commitIndex {
		n.lastApplied = n.commitIndex
	}
	n.rebuildKVLocked()
	return n.rewriteLogLocked()
}

func (n *Node) rebuildKVLocked() {
	n.kv = make(map[string]string)
	for i := 1; i <= n.lastApplied && i < len(n.log); i++ {
		e := n.log[i]
		if e.Type == cmdSet {
			n.kv[e.Key] = e.Value
		}
	}
}

func (n *Node) rewriteLogLocked() error {
	if n.logFile != nil {
		_ = n.logFile.Close()
		n.logFile = nil
	}
	path := n.logPath()
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	for _, e := range n.log[1:] {
		b, err := json.Marshal(e)
		if err != nil {
			f.Close()
			return err
		}
		if _, err := f.Write(append(b, '\n')); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	_ = dir.Sync()
	dir.Close()
	lf, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	n.logFile = lf
	return nil
}

func (n *Node) fsyncAppendLocked(e LogEntry) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if _, err := n.logFile.Write(append(b, '\n')); err != nil {
		return err
	}
	return n.logFile.Sync()
}

func atomicWriteJSON(path string, v any) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
