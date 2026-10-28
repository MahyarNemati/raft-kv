// Package config loads node configuration from the environment.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/MahyarNemati/raft-kv/internal/raft"
)

// Config is the full runtime configuration of a raftkv node.
type Config struct {
	ID     raft.NodeID
	Peers  map[raft.NodeID]string // id -> base URL, including self
	Listen string
	DataDir string

	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
	RPCTimeout        time.Duration
	RequestTimeout    time.Duration
	SnapshotThreshold uint64
	Fsync             bool

	LogLevel  slog.Level
	LogFormat string // "json" or "text"
}

// PeerIDs returns the member IDs in a stable order.
func (c *Config) PeerIDs() []raft.NodeID {
	ids := make([]raft.NodeID, 0, len(c.Peers))
	for id := range c.Peers {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// Load reads configuration through getenv (os.Getenv in production) and
// validates it, reporting every problem at once.
func Load(getenv func(string) string) (*Config, error) {
	p := parser{getenv: getenv}
	c := &Config{
		ID:                raft.NodeID(p.str("RAFTKV_ID", "")),
		Listen:            p.str("RAFTKV_LISTEN", ":7000"),
		DataDir:           p.str("RAFTKV_DATA_DIR", "data"),
		ElectionTimeout:   p.duration("RAFTKV_ELECTION_TIMEOUT", 300*time.Millisecond),
		HeartbeatInterval: p.duration("RAFTKV_HEARTBEAT_INTERVAL", 50*time.Millisecond),
		RPCTimeout:        p.duration("RAFTKV_RPC_TIMEOUT", 200*time.Millisecond),
		RequestTimeout:    p.duration("RAFTKV_REQUEST_TIMEOUT", 5*time.Second),
		SnapshotThreshold: p.uint("RAFTKV_SNAPSHOT_THRESHOLD", 10000),
		Fsync:             p.bool("RAFTKV_FSYNC", true),
		LogFormat:         p.str("RAFTKV_LOG_FORMAT", "json"),
	}
	c.Peers = p.peers("RAFTKV_PEERS")
	c.LogLevel = p.level("RAFTKV_LOG_LEVEL", slog.LevelInfo)

	if c.ID == "" {
		p.fail("RAFTKV_ID is required")
	} else if len(c.Peers) > 0 {
		if _, ok := c.Peers[c.ID]; !ok {
			p.fail(fmt.Sprintf("RAFTKV_PEERS must include this node (%s)", c.ID))
		}
	}
	if c.LogFormat != "json" && c.LogFormat != "text" {
		p.fail(`RAFTKV_LOG_FORMAT must be "json" or "text"`)
	}
	if c.ElectionTimeout > 0 && c.HeartbeatInterval >= c.ElectionTimeout {
		p.fail("RAFTKV_HEARTBEAT_INTERVAL must be shorter than RAFTKV_ELECTION_TIMEOUT")
	}
	if c.DataDir == "" {
		p.fail("RAFTKV_DATA_DIR must not be empty")
	}
	if len(p.errs) > 0 {
		return nil, fmt.Errorf("config: %w", errors.Join(p.errs...))
	}
	return c, nil
}

// RaftConfig derives the consensus configuration.
func (c *Config) RaftConfig(logger *slog.Logger) raft.Config {
	rc := raft.DefaultConfig(c.ID, c.PeerIDs())
	rc.ElectionTimeout = c.ElectionTimeout
	rc.HeartbeatInterval = c.HeartbeatInterval
	rc.RPCTimeout = c.RPCTimeout
	rc.SnapshotThreshold = c.SnapshotThreshold
	rc.Logger = logger
	return rc
}

type parser struct {
	getenv func(string) string
	errs   []error
}

func (p *parser) fail(msg string) { p.errs = append(p.errs, errors.New(msg)) }

func (p *parser) str(key, def string) string {
	if v := strings.TrimSpace(p.getenv(key)); v != "" {
		return v
	}
	return def
}

func (p *parser) duration(key string, def time.Duration) time.Duration {
	v := p.getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		p.fail(fmt.Sprintf("%s: %q is not a positive duration", key, v))
		return def
	}
	return d
}

func (p *parser) uint(key string, def uint64) uint64 {
	v := p.getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		p.fail(fmt.Sprintf("%s: %q is not a non-negative integer", key, v))
		return def
	}
	return n
}

func (p *parser) bool(key string, def bool) bool {
	v := p.getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		p.fail(fmt.Sprintf("%s: %q is not a boolean", key, v))
		return def
	}
	return b
}

func (p *parser) level(key string, def slog.Level) slog.Level {
	v := p.getenv(key)
	if v == "" {
		return def
	}
	var l slog.Level
	if err := l.UnmarshalText([]byte(v)); err != nil {
		p.fail(fmt.Sprintf("%s: %q is not a log level", key, v))
		return def
	}
	return l
}

// peers parses "n1=http://host1:7000,n2=http://host2:7000".
func (p *parser) peers(key string) map[raft.NodeID]string {
	v := p.getenv(key)
	if strings.TrimSpace(v) == "" {
		p.fail(key + " is required")
		return nil
	}
	out := make(map[raft.NodeID]string)
	for _, part := range strings.Split(v, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		if !ok || id == "" || addr == "" {
			p.fail(fmt.Sprintf("%s: %q is not id=url", key, part))
			continue
		}
		u, err := url.Parse(addr)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			p.fail(fmt.Sprintf("%s: %q is not an http(s) URL", key, addr))
			continue
		}
		if _, dup := out[raft.NodeID(id)]; dup {
			p.fail(fmt.Sprintf("%s: duplicate id %q", key, id))
			continue
		}
		out[raft.NodeID(id)] = strings.TrimRight(addr, "/")
	}
	return out
}
