package main

import (
	"context"
	pb "local/crawler/gen/queue"
	"log"
	"net"
	"os/signal"
	"os"
	"syscall"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

type Queue struct {
	pb.UnimplementedQueueServiceServer
	redisClient *redis.Client
}

func (q *Queue) Push(ctx context.Context, req *pb.PushRequest) (*pb.PushResponse, error) {
	for _, url := range req.Urls {
		err := q.redisClient.LPush(ctx, req.QueueName, url).Err()
		if err != nil {
			return &pb.PushResponse{Success: false}, err
		}
	}
	return &pb.PushResponse{Success: true}, nil
}

func (q *Queue) Pop(ctx context.Context, req *pb.PopRequest) (*pb.PopResponse, error) {
	result, err := q.redisClient.RPop(ctx, req.QueueName).Result()
	if err == redis.Nil {
		return &pb.PopResponse{
			Urls:    []string{},
			Success: true,
		}, nil
	} else if err != nil {
		log.Printf("Redis error: %v", err)
		return &pb.PopResponse{Success: false}, err
	}

	return &pb.PopResponse{
		Urls:    []string{result},
		Success: true,
	}, nil
}

func (q *Queue) GetQueueSize(ctx context.Context, req *pb.QueueSizeRequest) (*pb.QueueSizeResponse, error) {
	size, err := q.redisClient.LLen(ctx, req.QueueName).Result()
	if err != nil {
		return &pb.QueueSizeResponse{Success: false}, err
	}
	
	items, err := q.redisClient.LRange(ctx, req.QueueName, 0, 0).Result()
	if err != nil {
		return &pb.QueueSizeResponse{Success: false}, err
	}
	
	return &pb.QueueSizeResponse{
		QueueName:  req.QueueName,
		Size:       int32(size),
		FirstItems: items,
		Success:    true,
	}, nil
}
func (q *Queue) ClearQueue(ctx context.Context, req *pb.ClearQueueRequest) (*pb.ClearQueueResponse, error) {
    err := q.redisClient.Del(ctx, req.QueueName).Err()
    if err != nil {
        return &pb.ClearQueueResponse{Success: false}, err
    }
    return &pb.ClearQueueResponse{Success: true}, nil
}

func main() {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	
	ctx := context.Background()
	_, err := client.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("❌ Redis connection failed: %v", err)
	}
	log.Printf("✅ Redis connected successfully")

	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	
	grpcServer := grpc.NewServer()
	queueServer := &Queue{
		redisClient: client,
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-stop
		log.Println("🛑 Received shutdown signal...")
		log.Println("⏳ Gracefully stopping gRPC server...")
		grpcServer.GracefulStop()
		log.Println("✅ gRPC server stopped")
	}()

	pb.RegisterQueueServiceServer(grpcServer, queueServer)
	log.Println("Queue Service is running on port 50052")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
