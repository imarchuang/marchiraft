# marchiraft

Educational Raft-inspired **replicated KV** in Go.

One process per node, inspectable on-disk logs, HTTP-first, Docker-runnable.
Built as small slices rather than a HashiCorp Raft / etcd clone.

**Not etcd.** The goal is to learn **leader election + replicated log + commit
index** with a running 3-node cluster.

The slice plan is the source of truth: **[PLAN.md](PLAN.md)**.

---

## Quick start

**Go (1.22+):**

```bash
go test ./...
go run ./cmd/marchiraft -id=n1 -listen=:7001 -dataDir=./data/n1
```

**Docker (3 boxes):**

```bash
docker compose up --build
curl -s http://127.0.0.1:7001/healthz
curl -X PUT http://127.0.0.1:7001/kv/user -d alice
curl -s http://127.0.0.1:7001/kv/user
```

Slice 0 is a single-node in-memory map (no consensus yet). Clustering starts
in later slices — see PLAN.md.
