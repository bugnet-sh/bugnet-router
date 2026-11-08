// Command fakesshd stands in for sshd, checking its config and listening where the config says
package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
)

func main() {
	cfgPath, test := "", false
	args := os.Args[1:]
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-t":
			test = true
		case "-f":
			if i+1 < len(args) {
				i++
				cfgPath = args[i]
			}
		case "-D", "-e":
		default:
			die("unknown flag")
		}
	}
	cfg, err := readConfig(cfgPath)
	if err != nil || cfg["port"] == "" || cfg["pidfile"] == "" || cfg["listenaddress"] == "" {
		die("bad config")
	}
	if test {
		if _, err := os.ReadFile(cfg["hostkey"]); err != nil {
			die("no hostkeys available")
		}
		return
	}
	ln, err := net.Listen("tcp", net.JoinHostPort(cfg["listenaddress"], cfg["port"]))
	if err != nil {
		die("listen")
	}
	go serve(ln)
	pid := []byte(strconv.Itoa(os.Getpid()) + "\n")
	if err := os.WriteFile(cfg["pidfile"], pid, 0o644); err != nil {
		die("pid file")
	}
	signals := make(chan os.Signal, 4)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGTERM)
	for sig := range signals {
		if sig == syscall.SIGTERM {
			_ = os.Remove(cfg["pidfile"])
			return
		}
		// sshd rewrites its pid file once it has read its config again
		_ = os.WriteFile(cfg["pidfile"], pid, 0o644)
	}
}

func serve(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		_, _ = c.Write([]byte("SSH-2.0-fake\r\n"))
		_ = c.Close()
	}
}

// readConfig returns the first value of each keyword in lower case, as sshd reads them
func readConfig(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	cfg := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		k, v, _ := strings.Cut(strings.TrimSpace(sc.Text()), " ")
		k = strings.ToLower(k)
		if _, ok := cfg[k]; !ok && k != "" && !strings.HasPrefix(k, "#") {
			cfg[k] = v
		}
	}
	return cfg, sc.Err()
}

func die(msg string) {
	_, _ = fmt.Fprintln(os.Stderr, "fakesshd: "+msg)
	os.Exit(1)
}
