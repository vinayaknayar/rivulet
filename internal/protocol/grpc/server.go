package grpcproto

import (
	"context"
	"time"

	pb "rivulet/api/proto"
	"rivulet/internal/broker"
)

// Server is the gRPC edge: a thin translation layer between generated
// protobuf types and the broker's protocol-neutral request/response types.
// This is the whole point of the broker/edge split — adding a new protocol
// means writing a file this size, not touching storage or topic code.
type Server struct {
	pb.UnimplementedBrokerServer
	Broker *broker.Broker
}

func New(b *broker.Broker) *Server { return &Server{Broker: b} }

func (s *Server) Produce(_ context.Context, req *pb.ProduceRequest) (*pb.ProduceResponse, error) {
	r, err := s.Broker.Produce(broker.ProduceRequest{
		Topic: req.Topic, Partition: req.Partition, Key: req.Key, Value: req.Value,
	})
	if err != nil {
		return nil, err
	}
	return &pb.ProduceResponse{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset}, nil
}

// Fetch is a server-streaming RPC. It drains records currently available at
// or after the requested offset; then, if follow==true, it polls forever
// forwarding new records as they appear until the client cancels.
func (s *Server) Fetch(req *pb.FetchRequest, stream pb.Broker_FetchServer) error {
	offset := req.Offset
	maxBytes := int64(req.MaxBytes)
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	for {
		resp, err := s.Broker.Fetch(broker.FetchRequest{
			Topic: req.Topic, Partition: req.Partition, Offset: offset, MaxBytes: maxBytes,
		})
		if err != nil {
			return err
		}
		for _, r := range resp.Records {
			if err := stream.Send(&pb.Record{
				Offset: r.Offset, Timestamp: r.Timestamp, Key: r.Key, Value: r.Value,
			}); err != nil {
				return err
			}
			offset = r.Offset + 1
		}
		if len(resp.Records) > 0 {
			continue
		}
		if !req.Follow {
			return nil
		}
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (s *Server) Metadata(_ context.Context, _ *pb.MetadataRequest) (*pb.MetadataResponse, error) {
	resp := s.Broker.Metadata()
	out := &pb.MetadataResponse{}
	for _, t := range resp.Topics {
		out.Topics = append(out.Topics, &pb.TopicMeta{Name: t.Name, Partitions: t.Partitions})
	}
	return out, nil
}

func (s *Server) CreateTopic(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	if err := s.Broker.CreateTopic(req.Name, req.Partitions); err != nil {
		return nil, err
	}
	return &pb.CreateTopicResponse{}, nil
}
