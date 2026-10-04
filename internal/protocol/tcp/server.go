package tcpproto

import (
	"bufio"
	"errors"
	"io"
	"log"
	"net"

	"rivulet/internal/broker"
)

type Server struct {
	addr   string
	broker *broker.Broker
	ln     net.Listener
}

func NewServer(addr string, b *broker.Broker) *Server {
	return &Server{addr: addr, broker: b}
}

// Listen opens the listener but does not accept yet. Returns the resolved
// address so the caller can log it (useful when port=0).
func (s *Server) Listen() (string, error) {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return "", err
	}
	s.ln = ln
	return ln.Addr().String(), nil
}

func (s *Server) Serve() error {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go s.handle(conn)
	}
}

func (s *Server) Close() error {
	if s.ln == nil {
		return nil
	}
	return s.ln.Close()
}

func (s *Server) handle(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	for {
		apiKey, corr, payload, err := ReadFrame(r)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				log.Printf("tcp %s: read frame: %v", conn.RemoteAddr(), err)
			}
			return
		}
		resp := s.dispatch(apiKey, payload)
		if err := WriteFrame(w, apiKey, corr, resp); err != nil {
			log.Printf("tcp %s: write: %v", conn.RemoteAddr(), err)
			return
		}
		if err := w.Flush(); err != nil {
			log.Printf("tcp %s: flush: %v", conn.RemoteAddr(), err)
			return
		}
	}
}

func (s *Server) dispatch(apiKey uint16, payload []byte) []byte {
	switch apiKey {
	case APIProduce:
		return s.handleProduce(payload)
	case APIFetch:
		return s.handleFetch(payload)
	case APIMetadata:
		return s.handleMetadata(payload)
	case APICreateTopic:
		return s.handleCreateTopic(payload)
	default:
		out := NewBuf()
		out.PutErr(ErrBadRequest, "unknown api key")
		return out.Bytes()
	}
}

func (s *Server) handleProduce(payload []byte) []byte {
	r := NewReader(payload)
	topicName := r.String()
	partition := r.I32()
	key := r.BytesI32()
	value := r.BytesI32()
	out := NewBuf()
	if err := r.Err(); err != nil {
		out.PutErr(ErrBadRequest, err.Error())
		return out.Bytes()
	}
	resp, err := s.broker.Produce(broker.ProduceRequest{
		Topic:     topicName,
		Partition: partition,
		Key:       key,
		Value:     value,
	})
	if err != nil {
		out.PutErr(ErrUnknown, err.Error())
		return out.Bytes()
	}
	out.PutErr(ErrOK, "")
	out.WriteString(resp.Topic)
	out.WriteI32(resp.Partition)
	out.WriteI64(resp.Offset)
	return out.Bytes()
}

func (s *Server) handleFetch(payload []byte) []byte {
	r := NewReader(payload)
	topicName := r.String()
	partition := r.I32()
	offset := r.I64()
	maxBytes := int64(r.I32())
	out := NewBuf()
	if err := r.Err(); err != nil {
		out.PutErr(ErrBadRequest, err.Error())
		return out.Bytes()
	}
	resp, err := s.broker.Fetch(broker.FetchRequest{
		Topic:     topicName,
		Partition: partition,
		Offset:    offset,
		MaxBytes:  maxBytes,
	})
	if err != nil {
		out.PutErr(ErrUnknown, err.Error())
		return out.Bytes()
	}
	out.PutErr(ErrOK, "")
	out.WriteI64(resp.HighWatermark)
	out.WriteI32(int32(len(resp.Records)))
	for _, rec := range resp.Records {
		out.WriteI64(rec.Offset)
		out.WriteI64(rec.Timestamp)
		out.WriteBytesI32(rec.Key)
		out.WriteBytesI32(rec.Value)
	}
	return out.Bytes()
}

func (s *Server) handleMetadata(_ []byte) []byte {
	resp := s.broker.Metadata()
	out := NewBuf()
	out.PutErr(ErrOK, "")
	out.WriteI32(int32(len(resp.Topics)))
	for _, tm := range resp.Topics {
		out.WriteString(tm.Name)
		out.WriteI32(tm.Partitions)
	}
	return out.Bytes()
}

func (s *Server) handleCreateTopic(payload []byte) []byte {
	r := NewReader(payload)
	name := r.String()
	partitions := r.I32()
	out := NewBuf()
	if err := r.Err(); err != nil {
		out.PutErr(ErrBadRequest, err.Error())
		return out.Bytes()
	}
	if err := s.broker.CreateTopic(name, partitions); err != nil {
		out.PutErr(ErrUnknown, err.Error())
		return out.Bytes()
	}
	out.PutErr(ErrOK, "")
	return out.Bytes()
}
