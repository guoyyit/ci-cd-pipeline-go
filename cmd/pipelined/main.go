// Command pipelined runs the local CI/CD pipeline daemon: it watches a local
// Git repository, triggers pipeline runs from commit/tag events and serves a
// small localhost HTTP control API for pipelinectl.
//
// Usage:
//
//	pipelined -config pipeline.json [-data .pipelined] [-addr 127.0.0.1:8080]
//	          [-recovery resume|cancel|fail] [-interval 2s]
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/config"
	"github.com/guoyyit/ci-cd-pipeline-go/internal/engine"
	"github.com/guoyyit/ci-cd-pipeline-go/internal/git"
	"github.com/guoyyit/ci-cd-pipeline-go/internal/model"
	"github.com/guoyyit/ci-cd-pipeline-go/internal/server"
	"github.com/guoyyit/ci-cd-pipeline-go/internal/store"
)

func main() {
	cfgPath := flag.String("config", "pipeline.json", "pipeline configuration file")
	dataDir := flag.String("data", ".pipelined", "state directory")
	addr := flag.String("addr", "127.0.0.1:8080", "control API listen address")
	recovery := flag.String("recovery", "resume", "recovery policy for unfinished runs: resume|cancel|fail")
	interval := flag.Duration("interval", 2*time.Second, "git poll interval")
	flag.Parse()

	logger := log.New(os.Stderr, "pipelined: ", log.LstdFlags)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load(ctx, *cfgPath)
	if err != nil {
		logger.Fatalf("load config: %v", err)
	}

	st, err := store.NewFileStore(*dataDir)
	if err != nil {
		logger.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }()

	eng := engine.New(engine.Options{
		Store:        st,
		WorkDir:      cfg.Repo,
		Concurrency:  cfg.Concurrency,
		ArtifactRoot: *dataDir,
	})
	if err := eng.Start(ctx); err != nil {
		logger.Fatalf("start engine: %v", err)
	}

	policy := engine.RecoveryPolicy(*recovery)
	recovered, err := eng.Recover(ctx, policy)
	if err != nil {
		logger.Fatalf("recovery: %v", err)
	}
	if len(recovered) > 0 {
		logger.Printf("recovered %d unfinished run(s) with policy %q: %v", len(recovered), policy, recovered)
	}

	events := make(chan model.Event, 64)
	w := git.NewWatcher(cfg.Repo, *interval, nil)
	go func() {
		if err := w.Run(ctx, events); err != nil {
			logger.Printf("watcher stopped: %v", err)
		}
	}()

	go func() {
		for ev := range events {
			runID, created, err := eng.TriggerEvent(ctx, cfg, ev)
			if err != nil {
				logger.Printf("event %s failed: %v", ev.ID, err)
				continue
			}
			if created {
				ref := ev.Branch
				if ev.Tag != "" {
					ref = "tag " + ev.Tag
				}
				logger.Printf("triggered run %s for %s %s (%s)", runID, ev.Kind, ref, shortSHA(ev.Commit))
			} else {
				logger.Printf("event ignored (filtered or duplicate): %s", ev.ID)
			}
		}
	}()

	srv := &http.Server{Addr: *addr, Handler: server.New(eng).Handler()}
	go func() {
		logger.Printf("listening on %s, watching %s", *addr, cfg.Repo)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("http server: %v", err)
		}
	}()

	<-ctx.Done()
	logger.Printf("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Printf("http shutdown: %v", err)
	}
	if err := eng.Stop(shutdownCtx); err != nil {
		logger.Printf("engine stop: %v", err)
	}
	logger.Printf("active child processes at exit: %d", eng.ActiveChildren())
}

func shortSHA(sha string) string {
	if len(sha) > 8 {
		return sha[:8]
	}
	return sha
}
