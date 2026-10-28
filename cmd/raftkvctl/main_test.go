package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/MahyarNemati/raft-kv/internal/cluster"
)

func TestCLI(t *testing.T) {
	c, err := cluster.Start(cluster.Options{Size: 3, Seed: 3, HTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.WaitLeader(ctx); err != nil {
		t.Fatal(err)
	}
	eps := "-endpoints=" + strings.Join(c.Endpoints(), ",")

	cases := []struct {
		args []string
		code int
		out  string
	}{
		{[]string{"get", "k"}, exitNotFound, ""},
		{[]string{"put", "k", "v1"}, exitOK, "OK"},
		{[]string{"get", "k"}, exitOK, "v1"},
		{[]string{"cas", "k", "nope", "v2"}, exitNoSwap, "current value: v1"},
		{[]string{"cas", "k", "v1", "v2"}, exitOK, "OK"},
		{[]string{"cas", "-absent", "k", "v3"}, exitNoSwap, "current value: v2"},
		{[]string{"get", "-stale", "k"}, exitOK, ""},
		{[]string{"delete", "k"}, exitOK, "OK"},
		{[]string{"delete", "k"}, exitNotFound, ""},
		{[]string{"status"}, exitOK, `"role":"leader"`},
		{[]string{"put", "only-key"}, exitError, ""},
		{[]string{"frobnicate"}, exitError, ""},
		{[]string{}, exitError, ""},
	}
	for _, tc := range cases {
		var stdout, stderr bytes.Buffer
		code := run(append([]string{eps}, tc.args...), &stdout, &stderr)
		if code != tc.code {
			t.Fatalf("%v: exit %d, want %d (stderr: %s)", tc.args, code, tc.code, stderr.String())
		}
		if tc.out != "" && !strings.Contains(stdout.String(), tc.out) {
			t.Fatalf("%v: stdout %q, want it to contain %q", tc.args, stdout.String(), tc.out)
		}
	}
}
