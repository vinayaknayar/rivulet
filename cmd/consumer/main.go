package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	pb "rivulet/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "localhost:9093", "broker gRPC address")
	topic := flag.String("topic", "", "topic to consume")
	partition := flag.Int("partition", 0, "partition to consume")
	offset := flag.Int64("offset", 0, "starting offset")
	follow := flag.Bool("follow", true, "keep streaming new records after catching up")
	flag.Parse()

	if *topic == "" {
		log.Fatal("--topic is required")
	}

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := pb.NewBrokerClient(conn)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	stream, err := client.Fetch(ctx, &pb.FetchRequest{
		Topic: *topic, Partition: int32(*partition), Offset: *offset,
		MaxBytes: 1 << 20, Follow: *follow,
	})
	if err != nil {
		log.Fatal(err)
	}

	for {
		rec, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Fatal(err)
		}
		key := ""
		if len(rec.Key) > 0 {
			key = string(rec.Key)
		}
		fmt.Printf("[p%d off=%d ts=%d] key=%q value=%s\n", *partition, rec.Offset, rec.Timestamp, key, string(rec.Value))
	}
}
