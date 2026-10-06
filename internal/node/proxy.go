package node

import (
	"io"
	"net/http"
	"time"
)

func (n *Node) proxyToLeader(w http.ResponseWriter, r *http.Request, leaderID string) {
	if leaderID == "" || leaderID == n.cfg.ID {
		http.Error(w, "no leader", http.StatusServiceUnavailable)
		return
	}
	addr, ok := n.cfg.Peers[leaderID]
	if !ok || addr == "" {
		http.Error(w, "unknown leader "+leaderID, http.StatusServiceUnavailable)
		return
	}
	url := "http://" + addr + r.URL.Path
	if r.URL.RawQuery != "" {
		url += "?" + r.URL.RawQuery
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, url, r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	req.Header = r.Header.Clone()
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		http.Error(w, "proxy: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}
