// Command raftkvctl is a command-line client for a raftkv cluster.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/MahyarNemati/raft-kv/internal/client"
	"github.com/MahyarNemati/raft-kv/internal/kv"
)

const defaultEndpoints = "http://127.0.0.1:7001,http://127.0.0.1:7002,http://127.0.0.1:7003"

const usage = `usage: raftkvctl [flags] <command> [args]

commands:
  get [-stale] <key>              print the value of key
  put <key> <value>               set key
  delete <key>                    remove key
  cas <key> <expected> <value>    set key if it currently equals expected
  cas -absent <key> <value>       set key only if it does not exist
  status                          show every node's Raft status

flags:
`

// exit codes
const (
	exitOK       = 0
	exitError    = 1
	exitNotFound = 2
	exitNoSwap   = 3
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("raftkvctl", flag.ContinueOnError)
	fs.SetOutput(stderr)
	endpoints := fs.String("endpoints", envOr("RAFTKV_ENDPOINTS", defaultEndpoints), "comma-separated node URLs (env RAFTKV_ENDPOINTS)")
	timeout := fs.Duration("timeout", 5*time.Second, "overall request deadline")
	fs.Usage = func() {
		fmt.Fprint(stderr, usage)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return exitError
	}
	if fs.NArg() == 0 {
		fs.Usage()
		return exitError
	}

	cl, err := client.New(strings.Split(*endpoints, ","))
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitError
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	cmd, rest := fs.Arg(0), fs.Args()[1:]
	code, err := dispatch(ctx, cl, cmd, rest, stdout)
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		if errors.Is(err, client.ErrOutcomeUnknown) {
			fmt.Fprintln(stderr, "the write may or may not have been applied")
		}
	}
	return code
}

func dispatch(ctx context.Context, cl *client.Client, cmd string, args []string, out io.Writer) (int, error) {
	switch cmd {
	case "get":
		fs := flag.NewFlagSet("get", flag.ContinueOnError)
		stale := fs.Bool("stale", false, "read local state on any node without a quorum check")
		if err := fs.Parse(args); err != nil {
			return exitError, err
		}
		if fs.NArg() != 1 {
			return exitError, errors.New("get takes exactly one key")
		}
		cons := kv.Linearizable
		if *stale {
			cons = kv.Stale
		}
		v, found, err := cl.Get(ctx, fs.Arg(0), cons)
		if err != nil {
			return exitError, err
		}
		if !found {
			return exitNotFound, fmt.Errorf("key %q not found", fs.Arg(0))
		}
		fmt.Fprintln(out, v)
		return exitOK, nil

	case "put":
		if len(args) != 2 {
			return exitError, errors.New("put takes a key and a value")
		}
		if _, err := cl.Put(ctx, args[0], args[1]); err != nil {
			return exitError, err
		}
		fmt.Fprintln(out, "OK")
		return exitOK, nil

	case "delete", "del", "rm":
		if len(args) != 1 {
			return exitError, errors.New("delete takes exactly one key")
		}
		r, err := cl.Delete(ctx, args[0])
		if err != nil {
			return exitError, err
		}
		if !r.OK {
			return exitNotFound, fmt.Errorf("key %q not found", args[0])
		}
		fmt.Fprintln(out, "OK")
		return exitOK, nil

	case "cas":
		fs := flag.NewFlagSet("cas", flag.ContinueOnError)
		absent := fs.Bool("absent", false, "succeed only if the key does not exist")
		if err := fs.Parse(args); err != nil {
			return exitError, err
		}
		var (
			key, value string
			expected   *string
		)
		switch {
		case *absent && fs.NArg() == 2:
			key, value = fs.Arg(0), fs.Arg(1)
		case !*absent && fs.NArg() == 3:
			key, value = fs.Arg(0), fs.Arg(2)
			e := fs.Arg(1)
			expected = &e
		default:
			return exitError, errors.New("cas takes <key> <expected> <value>, or -absent <key> <value>")
		}
		r, err := cl.CAS(ctx, key, expected, value)
		if err != nil {
			return exitError, err
		}
		if !r.OK {
			cur := "<absent>"
			if r.Existed {
				cur = r.Prev
			}
			fmt.Fprintf(out, "not swapped; current value: %s\n", cur)
			return exitNoSwap, nil
		}
		fmt.Fprintln(out, "OK")
		return exitOK, nil

	case "status":
		code := exitOK
		enc := json.NewEncoder(out)
		for _, ep := range cl.Endpoints() {
			st, err := cl.Status(ctx, ep)
			if err != nil {
				st = map[string]any{"endpoint": ep, "error": err.Error()}
				code = exitError
			} else {
				st["endpoint"] = ep
			}
			if err := enc.Encode(st); err != nil {
				return exitError, err
			}
		}
		return code, nil

	default:
		return exitError, fmt.Errorf("unknown command %q", cmd)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
