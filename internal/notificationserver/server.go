package notificationserver

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var log = logf.Log.WithName("notification-server")

type NotificationServer struct {
	Client client.Client
	Port   int
}

func (s *NotificationServer) Start(ctx context.Context) error {
	handler := &notificationHandler{client: s.Client}

	mux := http.NewServeMux()
	mux.HandleFunc("/", handler.handleNotification)

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", s.Port),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Error(err, "notification server shutdown error")
		}
	}()

	log.Info("starting notification server", "port", s.Port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("notification server: %w", err)
	}
	return nil
}

func (s *NotificationServer) NeedLeaderElection() bool {
	return false
}
