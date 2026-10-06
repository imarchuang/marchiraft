# RAFT.md — how marchiraft maps to Raft

This is a learning note, not a protocol spec. Draw the three logs after a
failover and you should be able to point at `commitIndex` vs `lastLogIndex`.

## Replicated log, not “agree on the value”

A `PUT /kv/user` does **not** broadcast the map entry. The leader appends
`{index, term, SET, user, alice}` to its log, then `AppendEntries` copies that
**slot** to followers. The value exists in the state machine only after a
majority has the same index (`commitIndex` advances, then apply).

Inspect `{dataDir}/log/000001.jsonl` on each node. Same index must eventually
show the same term and command on a committed prefix. Tails can differ.

## Terms and votes

Each node stores `currentTerm` and `votedFor` in `meta.json`.

- Election timeout (randomized) → increment term, vote for self, `POST /raft/vote`.
- Vote is granted only if the candidate’s log is **at least as up-to-date**
  (last log term, then last index) — Raft §5.4.1.
- A follower that sees a higher term **steps down**. Heartbeats (`AppendEntries`
  with whatever entries the leader still owes) reset the election timer.

That last rule is why a stale leader cannot keep committing: its term is old,
followers reject, it steps down.

## commitIndex vs lastLogIndex

| Field | Meaning |
|---|---|
| `lastLogIndex` | Highest index **this** node wrote to disk (`logLength` on `/raft/status`) |
| `commitIndex` | Highest index a **majority** stored; safe to apply to `map[string]string` |
| `lastApplied` | Highest index already applied to the map |

The leader only advances `commitIndex` for an entry in **its current term**
(Raft Figure 8). Older-term entries ride along once a current-term entry
commits.

`GET /raft/status` dumps term, role, those indexes, peers, and (on the leader)
`nextIndex` / `matchIndex`.

## Why a longer uncommitted tail can lose

Suppose n1 (old leader) has:

```text
idx 1 term 1 SET user=alice   ← committed (majority)
idx 2 term 1 SET user=bob     ← only on n1; PUT timed out
```

n2 and n3 stopped before index 2. They elect in term 2 with lastLogIndex=1.
Their logs are **shorter** but **more committed**. §5.4.1 compares last term
then index: n1’s extra entry is the same term 1, higher index, so n1 could win
an election **if it were a voter in term 2 with that tail**.

In the MVP test we **kill n1** then let n2/n3 elect (majority of the original
three). The new leader’s log ends at alice. When n1 restarts, AppendEntries
at index 2 has a **different term** (the new SET). n1 truncates bob and
adopts the leader’s entry. Committed alice never moves.

When a new leader is elected it appends a **no-op** in the new term. That is
what lets `commitIndex` jump over previous-term entries (Figure 8) so a GET
after failover still sees alice even though volatile `commitIndex` reset.

## Linearizable KV (v0)

Writes and reads go through the leader. Followers **proxy** `/kv/*` so
`curl n2:7002/kv/user` still hits whoever is leader. There is no lease-read
shortcut.

## What we dropped

Member changes, pre-vote, snapshots, pipelined AppendEntries, watches, leases.
See [PLAN.md](PLAN.md).
