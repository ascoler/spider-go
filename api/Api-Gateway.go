package main

import (
	"context"
	pb "local/crawler/gen/crawler"
	"log"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
)

type Link struct {
	Url   string `json:"url"`
	Depth string `json:"depth"`
}

var crawlerClient pb.CrawlerServiceClient

func init() {
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatal("Connection failed:", err)
	}
	crawlerClient = pb.NewCrawlerServiceClient(conn)
}

func Analysis_Link(c *gin.Context) {
	var data Link

	if err := c.BindJSON(&data); err != nil {
		c.JSON(400, gin.H{"error": "Invalid JSON"})
		return
	}

	if data.Url == "" || (!strings.HasPrefix(data.Url, "http://") && !strings.HasPrefix(data.Url, "https://")) {
		c.JSON(400, gin.H{"error": "Invalid URL"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 300*time.Second)
	defer cancel()

	resp, err := crawlerClient.StartCrawling(ctx, &pb.StartCrawlingRequest{
		SeedUrls: []string{data.Url},
	})

	if err != nil {
		log.Printf("❌ Crawling error: %v", err)
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	log.Printf("✅ Success! Status: %s", resp.GetStatus())

	c.JSON(200, gin.H{
		"job_id":  resp.GetJobId(),
		"status":  resp.GetStatus(),
		"content": resp.GetContent(),
	})
}

func main() {
	r := gin.Default()
	r.POST("/Analysis_Link", Analysis_Link)

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "OK"})
	})

	log.Println("🚀 API Gateway starting on port 8080...")
	r.Run(":8080")
}
