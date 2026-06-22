package notificationserver

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/IBM/sarama"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	ceDispatch "github.com/functions-dev/mcg-adapter/internal/cloudevents"
)

var log = logf.Log.WithName("notification-server")

type NotificationServer struct {
	Client       client.Client
	Port         int
	KafkaBrokers []string
}

func (s *NotificationServer) Start(ctx context.Context) error {
	var kafkaProducer sarama.SyncProducer
	if len(s.KafkaBrokers) > 0 {
		var err error
		kafkaProducer, err = ceDispatch.NewKafkaProducer(s.KafkaBrokers)
		if err != nil {
			return fmt.Errorf("creating kafka producer: %w", err)
		}
		defer func() { _ = kafkaProducer.Close() }()
		log.Info("kafka producer initialized", "brokers", s.KafkaBrokers)
	}

	handler := &notificationHandler{client: s.Client, kafkaProducer: kafkaProducer}

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
