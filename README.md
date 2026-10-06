# marchiraft

Educational Raft-inspired **replicated KV** in Go.

One process per node, inspectable on-disk logs, HTTP-first, Docker-runnable.
Built as small slices rather than a HashiCorp Raft / etcd clone.

**Not etcd.** The goal is to learn **leader election + replicated log + commit
index** with a running 3-node cluster.

The slice plan is the source of truth: **[PLAN.md](PLAN.md)**.
Design notes: **[RAFT.md](RAFT.md)** (`commitIndex` vs `lastLogIndex`, stale
leaders, why a longer uncommitted tail can still lose).

---

## Quick start

**Go (1.22+):**

```bash
go test ./...
go run ./cmd/marchiraft -id=n1 -listen=:7001 \
  -peers=n1=127.0.0.1:7001,n2=127.0.0.1:7002,n3=127.0.0.1:7003 \
  -dataDir=./data/n1
```

**Docker (3 boxes):**

```bash
docker compose up --build
curl -s http://127.0.0.1:7001/healthz
curl -X PUT http://127.0.0.1:7001/kv/user -d alice
curl -s http://127.0.0.1:7002/kv/user          # proxied to the leader
curl -s http://127.0.0.1:7001/raft/status
./scripts/demo.sh                             # PUT, stop n1, GET via n2
```

Flags: `-id`, `-listen`, `-peers=id=host:port,...`, `-dataDir`.

On disk per node: `meta.json` (term, votedFor) and `log/000001.jsonl`.
