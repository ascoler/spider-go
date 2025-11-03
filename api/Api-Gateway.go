package main

import (
	"context"
	pb "local/crawler/internal/gen/crawler"
	"log"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
)

type Link struct {
	Url   string `json:"url"`   
	Depth string `json:"depth"` 
}

func Analysis_Link(c *gin.Context) {
	var data Link
	

	if err := c.BindJSON(&data); err != nil {
		c.JSON(400, gin.H{"error": "Invalid JSON"})
		return
	}

	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatal("Connection failed:", err)
	}
	defer conn.Close()
	
	client := pb.NewCrawlerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	resp, err := client.StartCrawling(ctx, &pb.StartCrawlingRequest{
		SeedUrls: []string{data.Url},
		Config: &pb.Config{
			MaxDepth:       3,
			MaxPages:       100,
			WorkerPoolSize: 10,
			RequestTimeout: 5,
			MaxRetries:     3,
			RetryDelay:     2,
			RateLimitDelay: 1,
			StorageType:    "in-memory",
			LogLevel:       "info",
			OutputFile:     "output.json",
		},
	})
	
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	
	log.Printf("✅ Success! Job ID: %s, Status: %s", resp.GetJobId(), resp.GetStatus())
	
	
	c.JSON(200, gin.H{
		"job_id": resp.GetJobId(),
		"status": resp.GetStatus(),
		"content": resp.GetContent(),
	})
}

func main() {
	r := gin.Default()
	r.POST("/Analysis_Link", Analysis_Link)
	r.Run(":8080")
}
