// Command raftkv runs one node of a raftkv cluster. Raft RPCs and the client
// API share a single HTTP listener; see internal/config for the environment
// variables it reads.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/MahyarNemati/raft-kv/internal/config"
	"github.com/MahyarNemati/raft-kv/internal/kv"
	"github.com/MahyarNemati/raft-kv/internal/raft"
	"github.com/MahyarNemati/raft-kv/internal/server"
	"github.com/MahyarNemati/raft-kv/internal/storage"
	"github.com/MahyarNemati/raft-kv/internal/transport/httpjson"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "raftkv:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return err
	}
	logger := newLogger(cfg)
	slog.SetDefault(logger)

	wal, err := storage.OpenWAL(cfg.DataDir, storage.WALOptions{NoSync: !cfg.Fsync, Logger: logger})
	if err != nil {
		return err
	}
	defer wal.Close()

	peers := make(map[raft.NodeID]string, len(cfg.Peers))
	for id, u := range cfg.Peers {
		if id != cfg.ID {
			peers[id] = u
		}
	}
	fsm := kv.NewFSM()
	node, err := raft.NewNode(cfg.RaftConfig(logger), fsm, wal, httpjson.NewClient(peers))
	if err != nil {
		return err
	}
	api := server.New(kv.NewStore(node, fsm), node, cfg.Peers, cfg.RequestTimeout, logger)

	mux := http.NewServeMux()
	mux.Handle("/raft/", httpjson.Handler(node))
	mux.Handle("/", api.Handler())
	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	node.Start()
	errc := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", cfg.Listen, "id", string(cfg.ID), "peers", len(cfg.Peers), "data_dir", cfg.DataDir)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
		close(errc)
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutting down")
	case err := <-errc:
		node.Stop()
		return fmt.Errorf("http server: %w", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Warn("http shutdown", "err", err)
	}
	node.Stop()
	if err := node.Err(); err != nil {
		return err
	}
	return nil
}

func newLogger(cfg *config.Config) *slog.Logger {
	opts := &slog.HandlerOptions{Level: cfg.LogLevel}
	if cfg.LogFormat == "text" {
		return slog.New(slog.NewTextHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, opts))
}
