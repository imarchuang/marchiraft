package node

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

const (
	defaultElection  = 150 * time.Millisecond
	defaultHeartbeat = 50 * time.Millisecond
	tickInterval     = 10 * time.Millisecond
)

func (n *Node) Start() {
	n.mu.Lock()
	if n.started {
		n.mu.Unlock()
		return
	}
	n.started = true
	n.resetElectionLocked()
	n.nextHeartbeat = time.Now()
	n.mu.Unlock()
	n.wg.Add(1)
	go n.run()
}

func (n *Node) run() {
	defer n.wg.Done()
	t := time.NewTicker(tickInterval)
	defer t.Stop()
	for {
		select {
		case <-n.stop:
			return
		case now := <-t.C:
			n.onTick(now)
		}
	}
}

func (n *Node) onTick(now time.Time) {
	n.mu.Lock()
	role := n.role
	electDue := role != RoleLeader && !n.nextElection.IsZero() && now.After(n.nextElection)
	hbDue := role == RoleLeader && now.After(n.nextHeartbeat)
	if hbDue {
		n.nextHeartbeat = now.Add(n.heartbeatIntervalLocked())
	}
	n.mu.Unlock()
	if electDue {
		n.startElection()
	}
	if hbDue {
		n.broadcastHeartbeat()
	}
}

func (n *Node) electionTimeoutLocked() time.Duration {
	base := n.cfg.ElectionTimeout
	if base == 0 {
		base = defaultElection
	}
	return base + time.Duration(rand.Int63n(int64(base)))
}

func (n *Node) heartbeatIntervalLocked() time.Duration {
	if n.cfg.HeartbeatInterval > 0 {
		return n.cfg.HeartbeatInterval
	}
	return defaultHeartbeat
}

func (n *Node) resetElectionLocked() {
	n.nextElection = time.Now().Add(n.electionTimeoutLocked())
}

func (n *Node) voterCountLocked() int {
	if len(n.cfg.Peers) == 0 {
		return 1
	}
	if _, ok := n.cfg.Peers[n.cfg.ID]; ok {
		return len(n.cfg.Peers)
	}
	return len(n.cfg.Peers) + 1
}

func (n *Node) majorityLocked() int {
	return n.voterCountLocked()/2 + 1
}

func (n *Node) becomeFollowerLocked(term int) {
	n.term = term
	n.role = RoleFollower
	n.votedFor = ""
	n.leaderID = ""
	_ = n.persistMetaLocked()
	n.resetElectionLocked()
}

func (n *Node) becomeLeaderLocked() {
	n.role = RoleLeader
	n.leaderID = n.cfg.ID
	n.nextHeartbeat = time.Now()
	log.Printf("%s became leader term=%d", n.cfg.ID, n.term)
}

func (n *Node) startElection() {
	n.mu.Lock()
	if n.role == RoleLeader {
		n.mu.Unlock()
		return
	}
	n.term++
	n.role = RoleCandidate
	n.votedFor = n.cfg.ID
	n.leaderID = ""
	if err := n.persistMetaLocked(); err != nil {
		n.mu.Unlock()
		log.Printf("%s persist election: %v", n.cfg.ID, err)
		return
	}
	term := n.term
	last := n.log[len(n.log)-1]
	peers := copyStringMap(n.cfg.Peers)
	self := n.cfg.ID
	n.resetElectionLocked()
	n.mu.Unlock()

	log.Printf("%s starting election term=%d", self, term)
	req := VoteRequest{
		Term:         term,
		CandidateID:  self,
		LastLogIndex: last.Index,
		LastLogTerm:  last.Term,
	}
	var mu sync.Mutex
	votes := 1
	var wg sync.WaitGroup
	for id, addr := range peers {
		if id == self {
			continue
		}
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			var resp VoteResponse
			if err := n.postJSON(addr, "/raft/vote", req, &resp); err != nil {
				return
			}
			n.observeTerm(resp.Term)
			if resp.VoteGranted {
				mu.Lock()
				votes++
				mu.Unlock()
			}
		}(addr)
	}
	wg.Wait()

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.term != term || n.role != RoleCandidate {
		return
	}
	if votes >= n.majorityLocked() {
		n.becomeLeaderLocked()
	}
}

func (n *Node) observeTerm(term int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if term > n.term {
		n.becomeFollowerLocked(term)
	}
}

func (n *Node) onRequestVote(req VoteRequest) VoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	if req.Term > n.term {
		n.becomeFollowerLocked(req.Term)
	}
	resp := VoteResponse{Term: n.term}
	if req.Term < n.term {
		return resp
	}
	last := n.log[len(n.log)-1]
	upToDate := req.LastLogTerm > last.Term || (req.LastLogTerm == last.Term && req.LastLogIndex >= last.Index)
	if (n.votedFor == "" || n.votedFor == req.CandidateID) && upToDate {
		n.votedFor = req.CandidateID
		_ = n.persistMetaLocked()
		n.resetElectionLocked()
		resp.VoteGranted = true
	}
	return resp
}

func (n *Node) onAppendEntries(req AppendRequest) AppendResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	if req.Term < n.term {
		return AppendResponse{Term: n.term, Success: false}
	}
	if req.Term > n.term {
		n.becomeFollowerLocked(req.Term)
	} else if n.role != RoleFollower {
		n.role = RoleFollower
	}
	n.leaderID = req.LeaderID
	n.resetElectionLocked()
	lastIdx := n.log[len(n.log)-1].Index
	if req.PrevLogIndex > lastIdx || req.PrevLogIndex >= len(n.log) {
		return AppendResponse{Term: n.term, Success: false}
	}
	if n.log[req.PrevLogIndex].Term != req.PrevLogTerm {
		return AppendResponse{Term: n.term, Success: false}
	}
	return AppendResponse{Term: n.term, Success: true}
}

func (n *Node) broadcastHeartbeat() {
	n.mu.Lock()
	if n.role != RoleLeader {
		n.mu.Unlock()
		return
	}
	last := n.log[len(n.log)-1]
	req := AppendRequest{
		Term:         n.term,
		LeaderID:     n.cfg.ID,
		PrevLogIndex: last.Index,
		PrevLogTerm:  last.Term,
		LeaderCommit: n.commitIndex,
	}
	peers := copyStringMap(n.cfg.Peers)
	self := n.cfg.ID
	n.mu.Unlock()
	for id, addr := range peers {
		if id == self {
			continue
		}
		go func(addr string) {
			var resp AppendResponse
			if err := n.postJSON(addr, "/raft/append", req, &resp); err != nil {
				return
			}
			n.observeTerm(resp.Term)
		}(addr)
	}
}

func copyStringMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
