package main

import (
	"context"
	"fmt"
	pb "local/crawler/internal/gen/queue"
	"log"
	"net"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)
type Queue struct {
	pb.UnimplementedQueueServiceServer
	redisClient *redis.Client
}

var ctx = context.Background()
func checkConection(client *redis.Client) error {
	_,err := client.Ping(ctx).Result()
	if err != nil {
		return err
	}
	defer client.Close()
	return nil
}
func PushToQueue(client *redis.Client,queueName string, url string) bool {
	err := client.LPush(ctx,queueName,url).Err()
	
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	return true

}
func GetFromQueue(client *redis.Client, queueName string) (string, error) {
    result, err := client.RPop(ctx, queueName).Result()
    if err == redis.Nil {
        
        return "", nil
    } else if err != nil {
        log.Printf("Redis error: %v", err)
        return "", err
    }
    
    return result, nil
}
func (q *Queue) Pop(ctx context.Context, req *pb.PopRequest) (*pb.PopResponse, error) {
    
    result, err := GetFromQueue(q.redisClient, req.QueueName)
    if err != nil {
        log.Printf("Failed to pop from queue: %v", err)
        return nil,nil
    }

    if result == "" {
        
        return &pb.PopResponse{
            Urls:    []string{},
            Success: true,
        }, nil
    }

    return &pb.PopResponse{
        Urls:    []string{result},
        Success: true,
    }, nil
}
func (q *Queue) GetQueueSize(ctx context.Context,req *pb.QueueSizeRequest)(*pb.QueueSizeResponse,error){
	size,_ := q.redisClient.LLen(ctx,req.QueueName).Result()
	items,_ := q.redisClient.LRange(ctx,req.QueueName,0,0).Result()
	return &pb.QueueSizeResponse{
    QueueName:  req.QueueName,  
    Size:       int32(size),
    FirstItems: items,          
    Success:    true,
}, nil
}

func main(){
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		Password: "", 
		DB: 0,  
	})
	if checkConection(client) != nil {
		fmt.Println("Failed to connect to Redis")
		return
	}
	lis,err := net.Listen("tcp",":50052")
	if err != nil {
		log.Fatalf("Failed to listen: %v",err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterQueueServiceServer(grpcServer,&Queue{})
	log.Println("Queue Service is running on port 50052")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v",err)
	}
}
