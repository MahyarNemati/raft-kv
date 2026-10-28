package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadDefaults(t *testing.T) {
	c, err := Load(env(map[string]string{
		"RAFTKV_ID":    "n1",
		"RAFTKV_PEERS": "n1=http://a:7000, n2=http://b:7000/ ,n3=http://c:7000",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if c.Listen != ":7000" || c.ElectionTimeout != 300*time.Millisecond || !c.Fsync || c.LogLevel != slog.LevelInfo {
		t.Fatalf("defaults wrong: %+v", c)
	}
	if c.Peers["n2"] != "http://b:7000" {
		t.Fatalf("trailing slash not trimmed: %q", c.Peers["n2"])
	}
	ids := c.PeerIDs()
	if len(ids) != 3 || ids[0] != "n1" || ids[2] != "n3" {
		t.Fatalf("peer ids = %v", ids)
	}
	rc := c.RaftConfig(slog.Default())
	if rc.ID != "n1" || len(rc.Peers) != 3 || rc.SnapshotThreshold != 10000 {
		t.Fatalf("raft config = %+v", rc)
	}
}

func TestLoadOverrides(t *testing.T) {
	c, err := Load(env(map[string]string{
		"RAFTKV_ID":                 "n1",
		"RAFTKV_PEERS":              "n1=http://a:7000",
		"RAFTKV_ELECTION_TIMEOUT":   "1s",
		"RAFTKV_HEARTBEAT_INTERVAL": "100ms",
		"RAFTKV_FSYNC":              "false",
		"RAFTKV_LOG_LEVEL":          "debug",
		"RAFTKV_LOG_FORMAT":         "text",
		"RAFTKV_SNAPSHOT_THRESHOLD": "0",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if c.ElectionTimeout != time.Second || c.Fsync || c.LogLevel != slog.LevelDebug || c.SnapshotThreshold != 0 {
		t.Fatalf("overrides not applied: %+v", c)
	}
}

func TestLoadReportsAllErrors(t *testing.T) {
	_, err := Load(env(map[string]string{
		"RAFTKV_PEERS":              "n1=ftp://a,bogus,n2=http://b:1,n2=http://c:1",
		"RAFTKV_ELECTION_TIMEOUT":   "soon",
		"RAFTKV_FSYNC":              "maybe",
		"RAFTKV_LOG_FORMAT":         "xml",
		"RAFTKV_SNAPSHOT_THRESHOLD": "-1",
	}))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{
		"RAFTKV_ID is required",
		"not an http(s) URL",
		"not id=url",
		"duplicate id",
		"RAFTKV_ELECTION_TIMEOUT",
		"RAFTKV_FSYNC",
		"RAFTKV_LOG_FORMAT",
		"RAFTKV_SNAPSHOT_THRESHOLD",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q:\n%v", want, err)
		}
	}
}

func TestLoadRequiresSelfInPeers(t *testing.T) {
	_, err := Load(env(map[string]string{"RAFTKV_ID": "n9", "RAFTKV_PEERS": "n1=http://a:7000"}))
	if err == nil || !strings.Contains(err.Error(), "must include this node") {
		t.Fatalf("err = %v", err)
	}
}

func TestLoadHeartbeatBelowElection(t *testing.T) {
	_, err := Load(env(map[string]string{
		"RAFTKV_ID": "n1", "RAFTKV_PEERS": "n1=http://a:7000",
		"RAFTKV_ELECTION_TIMEOUT": "100ms", "RAFTKV_HEARTBEAT_INTERVAL": "100ms",
	}))
	if err == nil {
		t.Fatal("expected error")
	}
}
