// Command quota-server starts the multi-tenant quota engine HTTP service.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cuixudong74/tenant-quota-engine-go/internal/api"
	"github.com/cuixudong74/tenant-quota-engine-go/internal/engine"
	"github.com/cuixudong74/tenant-quota-engine-go/internal/observe"
	"github.com/cuixudong74/tenant-quota-engine-go/internal/rules"
	"github.com/cuixudong74/tenant-quota-engine-go/internal/store"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	dataDir := flag.String("data-dir", "./data", "directory for persisted state")
	flag.Parse()

	st, err := store.NewFileStore(*dataDir)
	if err != nil {
		log.Fatalf("failed to open store: %v", err)
	}
	defer st.Close()

	rm := rules.NewManager(st)
	if err := rm.Load(context.Background()); err != nil {
		log.Fatalf("failed to load rules: %v", err)
	}

	eng := engine.New(st, rm, engine.DefaultConfig())
	metrics := observe.NewMetrics(st, rm)
	srv := api.New(*addr, eng, metrics)

	go func() {
		log.Printf("quota-server listening on %s", *addr)
		if err := srv.ListenAndServe(); err != nil && err.Error() != "http: Server closed" {
			log.Fatalf("server error: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	_ = st.Close()
}
