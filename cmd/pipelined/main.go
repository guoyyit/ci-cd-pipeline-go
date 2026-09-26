package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/guoyyit/ci-cd-pipeline-go/internal/pipeline"
)

func main() {
	configPath := flag.String("config", "pipeline.json", "流水线配置文件路径")
	addr := flag.String("addr", ":8080", "HTTP API 监听地址")
	pollInterval := flag.Duration("poll", 2*time.Second, "Git 仓库轮询间隔")
	flag.Parse()

	cfg, err := pipeline.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	store, err := pipeline.OpenStore(filepath.Join(cfg.Settings.DataDir, "state.json"))
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer store.Close()

	exec := pipeline.NewExecutor(cfg.Repo.Path, cfg.Settings.DataDir)
	runner := pipeline.NewRunner(cfg, store, exec)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	runner.Start(ctx)

	watcher := pipeline.NewWatcher(cfg.Repo.Path, *pollInterval, store, func(e pipeline.Event) {
		run, created, err := runner.SubmitEvent(e)
		if err != nil {
			log.Printf("submit event %s: %v", e.ID, err)
			return
		}
		switch {
		case run == nil:
			log.Printf("event %s ignored (filtered by trigger rules)", e.ID)
		case created:
			log.Printf("event %s created run %s", e.ID, run.ID)
		default:
			log.Printf("event %s duplicate, run %s reused", e.ID, run.ID)
		}
	})
	go watcher.Run(ctx)

	srv := &http.Server{Addr: *addr, Handler: pipeline.NewServer(runner).Handler()}
	go func() {
		log.Printf("pipeline %q listening on %s, watching repo %s", cfg.Name, *addr, cfg.Repo.Path)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down ...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	runner.Stop()
	fmt.Fprintln(os.Stderr, "stopped")
}
