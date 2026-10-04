package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "rivulet/api/proto"
	"rivulet/internal/broker"
	"rivulet/internal/config"
	grpcproto "rivulet/internal/protocol/grpc"
	tcpproto "rivulet/internal/protocol/tcp"

	"google.golang.org/grpc"
)

func main() {
	dataDir := flag.String("data", "data", "data directory")
	tcpAddr := flag.String("tcp", ":9092", "TCP listen address")
	grpcAddr := flag.String("grpc", ":9093", "gRPC listen address")
	segBytes := flag.Int64("segment-bytes", 1<<20, "roll segment when this size is exceeded")
	flag.Parse()

	cfg := config.Default()
	cfg.DataDir = *dataDir
	cfg.TCPListenAddr = *tcpAddr
	cfg.GRPCListenAddr = *grpcAddr
	cfg.SegmentBytes = *segBytes

	b, err := broker.New(cfg)
	if err != nil {
		log.Fatalf("broker: %v", err)
	}

	// TCP edge.
	tcpSrv := tcpproto.NewServer(cfg.TCPListenAddr, b)
	tcpAddrActual, err := tcpSrv.Listen()
	if err != nil {
		log.Fatalf("tcp listen: %v", err)
	}
	go func() {
		if err := tcpSrv.Serve(); err != nil {
			log.Printf("tcp serve: %v", err)
		}
	}()
	log.Printf("TCP listening on %s", tcpAddrActual)

	// gRPC edge.
	grpcLn, err := net.Listen("tcp", cfg.GRPCListenAddr)
	if err != nil {
		log.Fatalf("grpc listen: %v", err)
	}
	grpcSrv := grpc.NewServer()
	pb.RegisterBrokerServer(grpcSrv, grpcproto.New(b))
	go func() {
		if err := grpcSrv.Serve(grpcLn); err != nil {
			log.Printf("grpc serve: %v", err)
		}
	}()
	log.Printf("gRPC listening on %s", grpcLn.Addr().String())

	ctx, cancel := context.WithCancel(context.Background())
	go retentionLoop(ctx, b, cfg.RetentionCheckInterval)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("shutting down…")
	cancel()
	tcpSrv.Close()
	grpcSrv.GracefulStop()
	if err := b.Close(); err != nil {
		log.Printf("broker close: %v", err)
	}
	log.Println("bye")
}

func retentionLoop(ctx context.Context, b *broker.Broker, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b.EnforceRetention()
		}
	}
}
