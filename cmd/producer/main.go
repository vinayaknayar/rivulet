package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	tcpproto "rivulet/internal/protocol/tcp"
)

func main() {
	addr := flag.String("addr", "localhost:9092", "broker TCP address")
	topicName := flag.String("topic", "", "topic name")
	key := flag.String("key", "", "message key (empty => round-robin)")
	partition := flag.Int("partition", -1, "partition (-1 => broker picks)")
	createTopic := flag.Bool("create-topic", false, "create the topic and exit")
	partitions := flag.Int("partitions", 3, "partitions for --create-topic")
	flag.Parse()

	if *topicName == "" {
		log.Fatal("--topic is required")
	}
	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	w := bufio.NewWriter(conn)
	r := bufio.NewReader(conn)

	if *createTopic {
		p := tcpproto.NewBuf()
		p.WriteString(*topicName)
		p.WriteI32(int32(*partitions))
		if err := tcpproto.WriteFrame(w, tcpproto.APICreateTopic, 1, p.Bytes()); err != nil {
			log.Fatal(err)
		}
		w.Flush()
		_, _, resp, err := tcpproto.ReadFrame(r)
		if err != nil {
			log.Fatal(err)
		}
		rd := tcpproto.NewReader(resp)
		code := rd.U16()
		msg := rd.String()
		if code != tcpproto.ErrOK {
			log.Fatalf("create-topic failed: %s", msg)
		}
		fmt.Printf("created topic %q with %d partitions\n", *topicName, *partitions)
		return
	}

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)
	var corr uint32
	var sent int
	for scanner.Scan() {
		line := scanner.Bytes()
		value := make([]byte, len(line))
		copy(value, line)

		p := tcpproto.NewBuf()
		p.WriteString(*topicName)
		p.WriteI32(int32(*partition))
		var k []byte
		if *key != "" {
			k = []byte(*key)
		}
		p.WriteBytesI32(k)
		p.WriteBytesI32(value)
		corr++
		if err := tcpproto.WriteFrame(w, tcpproto.APIProduce, corr, p.Bytes()); err != nil {
			log.Fatal(err)
		}
		w.Flush()

		_, _, resp, err := tcpproto.ReadFrame(r)
		if err != nil {
			log.Fatal(err)
		}
		rd := tcpproto.NewReader(resp)
		code := rd.U16()
		errMsg := rd.String()
		if code != tcpproto.ErrOK {
			log.Fatalf("produce failed: %s", errMsg)
		}
		_ = rd.String() // topic echoed back
		part := rd.I32()
		off := rd.I64()
		fmt.Printf("produced partition=%d offset=%d value=%q\n", part, off, value)
		sent++
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	fmt.Fprintf(os.Stderr, "total produced: %d\n", sent)
}
