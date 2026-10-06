#!/bin/sh
# Graduation demo from PLAN.md: 3 nodes, commit a key, kill the leader, read it back.
set -eu
cd "$(dirname "$0")/.."

echo "==> docker compose up --build"
docker compose up -d --build

echo "==> wait for n1 healthz"
for i in 1 2 3 4 5 6 7 8 9 10; do
	if curl -fsS http://127.0.0.1:7001/healthz >/dev/null; then
		break
	fi
	sleep 1
done

echo "==> PUT user=alice via n1 (waits for majority commit)"
curl -fsS -X PUT http://127.0.0.1:7001/kv/user -d alice
echo
echo "==> /raft/status on n1"
curl -fsS http://127.0.0.1:7001/raft/status
echo

echo "==> stop n1 (old leader or follower — remaining majority continues)"
docker compose stop n1

echo "==> GET user via n2 (proxied to the new leader)"
sleep 2
curl -fsS http://127.0.0.1:7002/kv/user
echo
echo "==> /raft/status on n2"
curl -fsS http://127.0.0.1:7002/raft/status
echo
