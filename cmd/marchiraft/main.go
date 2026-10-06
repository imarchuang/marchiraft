package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/marchi/marchiraft/internal/node"
)

func main() {
	id := flag.String("id", envOr("MARCHIRAFT_ID", "n1"), "node id")
	listen := flag.String("listen", envOr("MARCHIRAFT_LISTEN", ":7001"), "listen address")
	peers := flag.String("peers", envOr("MARCHIRAFT_PEERS", ""), "id=host:port,... (unused until clustering)")
	dataDir := flag.String("dataDir", envOr("MARCHIRAFT_DATA", "/data"), "data directory (unused in slice 0)")
	flag.Parse()
	_ = dataDir

	n := node.New(node.Config{
		ID:     *id,
		Listen: *listen,
		Peers:  parsePeers(*peers),
	})
	log.Printf("marchiraft id=%s listen=%s (in-memory KV, no Raft yet)", *id, *listen)
	if err := http.ListenAndServe(*listen, n.Handler()); err != nil {
		log.Fatal(err)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func parsePeers(s string) map[string]string {
	out := map[string]string{}
	if s == "" {
		return out
	}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		if !ok || id == "" || addr == "" {
			log.Fatalf("bad -peers entry %q (want id=host:port)", part)
		}
		out[id] = addr
	}
	return out
}
