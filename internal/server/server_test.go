package server

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MahyarNemati/raft-kv/internal/kv"
	"github.com/MahyarNemati/raft-kv/internal/raft"
)

// fakeRaft applies proposals straight to the FSM, or fails with err.
type fakeRaft struct {
	fsm    *kv.FSM
	idx    uint64
	err    error
	status raft.Status
}

func (f *fakeRaft) Propose(_ context.Context, b []byte) (any, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.idx++
	return f.fsm.Apply(f.idx, b), nil
}

func (f *fakeRaft) ReadIndex(context.Context) (uint64, error) { return f.idx, f.err }
func (f *fakeRaft) Status() raft.Status                       { return f.status }

func newTestServer(t *testing.T) (*fakeRaft, *httptest.Server) {
	t.Helper()
	fsm := kv.NewFSM()
	fr := &fakeRaft{fsm: fsm, status: raft.Status{ID: "n1", Role: "leader", Leader: "n1"}}
	peers := map[raft.NodeID]string{"n1": "http://n1:7000", "n2": "http://n2:7000"}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := httptest.NewServer(New(kv.NewStore(fr, fsm), fr, peers, time.Second, logger).Handler())
	t.Cleanup(srv.Close)
	return fr, srv
}

func call(t *testing.T, method, url, body string) (int, map[string]any, http.Header) {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return resp.StatusCode, out, resp.Header
}

func TestCRUD(t *testing.T) {
	_, srv := newTestServer(t)
	u := srv.URL

	if code, body, _ := call(t, "GET", u+"/v1/kv/a/b", ""); code != 404 || body["found"] != false {
		t.Fatalf("get missing: %d %v", code, body)
	}
	if code, body, _ := call(t, "PUT", u+"/v1/kv/a/b", `{"value":"1"}`); code != 200 || body["ok"] != true {
		t.Fatalf("put: %d %v", code, body)
	}
	if code, body, _ := call(t, "GET", u+"/v1/kv/a/b", ""); code != 200 || body["value"] != "1" {
		t.Fatalf("get: %d %v", code, body)
	}
	if code, body, _ := call(t, "GET", u+"/v1/kv/a/b?consistency=stale", ""); code != 200 || body["value"] != "1" {
		t.Fatalf("stale get: %d %v", code, body)
	}
	if code, body, _ := call(t, "POST", u+"/v1/cas/a/b", `{"expected":"0","value":"2"}`); code != 200 || body["ok"] != false || body["prev"] != "1" {
		t.Fatalf("failed cas: %d %v", code, body)
	}
	if code, body, _ := call(t, "POST", u+"/v1/cas/a/b", `{"expected":"1","value":"2"}`); code != 200 || body["ok"] != true {
		t.Fatalf("cas: %d %v", code, body)
	}
	if code, body, _ := call(t, "DELETE", u+"/v1/kv/a/b", ""); code != 200 || body["ok"] != true {
		t.Fatalf("delete: %d %v", code, body)
	}
}

func TestErrors(t *testing.T) {
	fr, srv := newTestServer(t)
	u := srv.URL

	cases := []struct {
		name, method, path, body string
		code                     int
		errCode                  string
	}{
		{"bad json", "PUT", "/v1/kv/a", `{`, 400, CodeInvalid},
		{"unknown field", "PUT", "/v1/kv/a", `{"valu":"x"}`, 400, CodeInvalid},
		{"missing body", "PUT", "/v1/kv/a", ``, 400, CodeInvalid},
		{"bad consistency", "GET", "/v1/kv/a?consistency=weak", ``, 400, CodeInvalid},
		{"cas conflict flags", "POST", "/v1/cas/a", `{"expected":"x","expect_absent":true}`, 400, CodeInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, body, _ := call(t, tc.method, u+tc.path, tc.body)
			if code != tc.code || body["code"] != tc.errCode {
				t.Fatalf("got %d %v", code, body)
			}
		})
	}

	_, _, _ = call(t, "PUT", u+"/v1/kv/a", `{"value":"1","client_id":"c","seq":5}`)
	if code, body, _ := call(t, "PUT", u+"/v1/kv/a", `{"value":"2","client_id":"c","seq":4}`); code != 409 || body["code"] != CodeRejected {
		t.Fatalf("stale seq: %d %v", code, body)
	}

	fr.err = &raft.NotLeaderError{Leader: "n2"}
	code, body, hdr := call(t, "PUT", u+"/v1/kv/a", `{"value":"1"}`)
	if code != http.StatusMisdirectedRequest || body["leader_url"] != "http://n2:7000" || hdr.Get("X-Raft-Leader") != "http://n2:7000" {
		t.Fatalf("not leader: %d %v", code, body)
	}

	for err, want := range map[error]int{
		raft.ErrLeadershipLost:   503,
		raft.ErrStopped:          503,
		context.DeadlineExceeded: 504,
	} {
		fr.err = err
		if code, _, _ := call(t, "GET", u+"/v1/kv/a", ""); code != want {
			t.Errorf("%v: got %d, want %d", err, code, want)
		}
	}
}

func TestHealthAndReady(t *testing.T) {
	fr, srv := newTestServer(t)
	if code, _, _ := call(t, "GET", srv.URL+"/healthz", ""); code != 200 {
		t.Fatalf("healthz = %d", code)
	}
	if code, _, _ := call(t, "GET", srv.URL+"/readyz", ""); code != 200 {
		t.Fatalf("readyz = %d", code)
	}
	fr.status.Leader = ""
	if code, _, _ := call(t, "GET", srv.URL+"/readyz", ""); code != 503 {
		t.Fatalf("readyz without leader = %d", code)
	}
	if code, body, _ := call(t, "GET", srv.URL+"/v1/status", ""); code != 200 || body["id"] != "n1" {
		t.Fatalf("status = %d %v", code, body)
	}
}
