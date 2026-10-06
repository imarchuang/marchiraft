# marchiraft — Raft / etcd-inspired consensus MVP

Educational replicated KV in Go. Same spirit as [marchilogs](../marchilogs)
and [marchisql](../marchisql): **one learning goal per slice**, inspectable
on-disk logs, HTTP-first, Docker runnable, tests that kill a leader.

**Not etcd / not HashiCorp Raft.** We borrow **leader election + replicated
log + commit index** — not member-change protocol, snapshot shipping, or
etcd watch/lease APIs.

This is the DDIA hole: every other `marchi*` repo is a single process.
After this MVP you can contrast **log replication (Raft)** with
**leaderless quorum (marchidynamo)**.

---

## Learning goal

With a running 3-node cluster and files on disk:

1. Why consensus is a **replicated log**, not “agree on the current value”.
2. How **terms + votes** elect one leader; why a stale leader must step down.
3. **Commit** = majority has the entry; apply to state machine only then.
4. Linearizable `SET` goes through the leader; `GET` in v0 also goes through
   the leader (no lease-read shortcut).
5. Kill the leader → election → committed keys still there; uncommitted may vanish.

**Pass bar:** draw the log of 3 nodes after a failover; point at `commitIndex`
vs `lastLogIndex`; explain why a follower with a longer *uncommitted* tail
can still lose the election.

---

## Concepts we keep (and drop)

| Raft / etcd | marchiraft v0 | Deferred |
|---|---|---|
| Static cluster | 3 nodes in config file | Add/remove voter |
| Leader election | randomized timeout, RequestVote | pre-vote, check quorum |
| Log replication | AppendEntries, one entry or small batch | pipeline, flow control |
| Commit / apply | majority → apply to map | config-change entries |
| Persistent state | currentTerm, votedFor, log | snapshot + install snapshot |
| Client KV | HTTP GET/SET on any node (redirect to leader) | watches, leases, TXN |
| Linearizability | writes + reads via leader | follower read with lease |

**Non-goals:** KRaft, ZooKeeper ZAB, Multi-Paxos naming, Byzantine, SSL.

---

## Core loop

```text
Client                         Leader                         Followers
  |  POST /kv/x=1                |                                |
  +----------------------------->|  append log[term, idx, SET x=1]|
  |                              |  AppendEntries RPC ----------->|
  |                              |<---------- ok, ok (majority)   |
  |                              |  commitIndex++; apply map      |
  |  200 {"ok":true} <-----------+                                |
```

On leader death: followers timeout → RequestVote → new leader with
**up-to-date log** (Raft §5.4.1) → leftover uncommitted entries of old
leader are overwritten.

---

## On-disk layout (per node)

```text
{dataDir}/
  meta.json          # currentTerm, votedFor, nodeID
  log/
    000001.jsonl     # {index, term, type, key, value} append-only
  snapshot/          # empty in v0
```

State machine is an in-memory `map[string]string` rebuilt by replaying
the log on start (v0 has no snapshot).

---

## API (HTTP, one process per node)

| Method | Path | Purpose |
|---|---|---|
| GET | `/healthz` | liveness + `{role, term, commitIndex}` |
| GET | `/kv/{key}` | linearizable get (proxy to leader) |
| PUT | `/kv/{key}` | body = value; wait until applied |
| GET | `/raft/status` | term, role, log length, peers |
| POST | `/raft/vote` | RequestVote (internal) |
| POST | `/raft/append` | AppendEntries (internal) |

Flags: `-id`, `-listen`, `-peers=id=host:port,...`, `-dataDir`.

Default ports: `7001 7002 7003`.

---

## MVP slices

### Slice 0 — skeleton
Single node, in-memory map, no Raft. `PUT/GET`. Docker compose 3 empty boxes.

### Slice 1 — persistent log + term
Append local log, fsync, restart replays. No peers yet.

### Slice 2 — election
3 nodes, timeouts, RequestVote, one leader. Test: two nodes up → majority elects.

### Slice 3 — replicate + commit
SET only succeeds after majority AppendEntries. Apply to map at commitIndex.
Test: partition 1 follower; SET still commits; isolated follower log lags.

### Slice 4 — failover
Kill leader; new election; committed keys survive; client retries via 302/proxy.
Test: uncommitted entry on old leader is overwritten (Raft figure 7 style, simplified).

### Slice 5 — polish
`/raft/status`, logging, docker-compose demo script, `RAFT.md` design note.

---

## Demo (graduation)

```bash
docker compose up --build   # nodes n1 n2 n3

curl -X PUT n1:7001/kv/user -d alice     # 200 after commit
# kill n1
curl n2:7002/kv/user                     # alice (proxied to new leader)
```

---

## Relation to siblings

| Project | Contrast |
|---|---|
| **marchiq** | Kafka replication ≈ leader log + ISR, **not** Raft; no client linearizable KV |
| **marchidynamo** | Quorum + LWW, no single log; conflicting writes allowed |
| **marchisql** | Single-node SI; Raft is how you’d replicate *after* SI exists |

Start at **slice 0** on `feat/skeleton`.
