package main

import (
	"context"
	"errors"
	"fmt"
	pb "local/crawler/gen/storage"
	"net"
	"time"
  
    "log/slog"
    "syscall"
    "os/signal"
    "os"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
    "google.golang.org/grpc"
    "google.golang.org/protobuf/types/known/timestamppb"
)

type Storage_Server struct {  
    pb.UnimplementedStorageServiceServer
    db *gorm.DB
}

type Page struct {
    ID          uint      `gorm:"primaryKey"`
    URL         string    `gorm:"uniqueIndex;not null;type:text;size:500"`
    StatusCode  *int
    Title       *string   `gorm:"type:text;index"`
    ContentText *string   `gorm:"type:text"`
    Depth       int       `gorm:"default:0"`
    CreatedAt   time.Time `gorm:"index"` 
    UpdatedAt   time.Time `gorm:"index"` 
    VisitedAt   *time.Time
    
}


func (s *Storage_Server) SavePage(ctx context.Context, req *pb.SavePageRequest) (*pb.SavePageResponse, error) {
    if req.GetPage() == nil {
        return &pb.SavePageResponse{
            Success: false,
            Message: "page is required",
        }, nil
    }
    
    page := req.GetPage()
    
    statusCode := int(page.GetStatusCode())
    title := page.GetTitle()
    contentText := page.GetContentText()
    
    dbPage := &Page{
        URL:         page.GetUrl(),
        StatusCode:  &statusCode,
        Title:       &title,
        ContentText: &contentText,
        Depth:       int(page.GetDepth()),
        VisitedAt:   timePtr(time.Now()),
    }
    
    err := s.db.Create(dbPage).Error
    if err != nil {
        return &pb.SavePageResponse{
            Success: false,
            Message: fmt.Sprintf("failed to save page: %v", err),
        }, nil
    }
    
    return &pb.SavePageResponse{
        Success: true,
        PageId:  fmt.Sprintf("%d", dbPage.ID),
        Message: "page saved successfully",
    }, nil
}

func (s *Storage_Server) CheckPageExists(ctx context.Context, req *pb.CheckPageRequest) (*pb.CheckPageResponse, error) {
    var count int64
    err := s.db.Model(&Page{}).Where("url = ?", req.GetUrl()).Count(&count).Error
    if err != nil {
        return &pb.CheckPageResponse{
            Exists: false,
        }, fmt.Errorf("database error: %v", err)
    }
    
    exists := count > 0
    
    var pageID string
    if exists {
        var page Page
        s.db.Where("url = ?", req.GetUrl()).First(&page)
        pageID = fmt.Sprintf("%d", page.ID)
    }
    
    return &pb.CheckPageResponse{
        Exists: exists,
        PageId: pageID,
    }, nil
}

func (s *Storage_Server) GetPage(ctx context.Context, req *pb.GetPageRequest) (*pb.GetPageResponse, error) {
    var page Page
    var err error
    
    if req.GetPageId() != "" {
        err = s.db.First(&page, req.GetPageId()).Error
    } else {
        err = s.db.Where("url = ?", req.GetUrl()).First(&page).Error
    }
    
    if err != nil {
        if errors.Is(err, gorm.ErrRecordNotFound) {
            return &pb.GetPageResponse{
                Success: false,
            }, nil
        }
        return &pb.GetPageResponse{
            Success: false,
        }, fmt.Errorf("database error: %v", err)
    }
    
    pbPage := &pb.Page{
        Id:          fmt.Sprintf("%d", page.ID),
        Url:         page.URL,
        StatusCode:  int32(getIntValue(page.StatusCode)),
        Title:       getStringValue(page.Title),
        ContentText: getStringValue(page.ContentText),
        Depth:       int32(page.Depth),
        IsProcessed: true,
        CreatedAt:   timeToTimestamp(page.CreatedAt),
        UpdatedAt:   timeToTimestamp(page.UpdatedAt),
        VisitedAt:   timeToTimestampPtr(page.VisitedAt),
    }
    
    return &pb.GetPageResponse{
        Page:    pbPage,
        Success: true,
    }, nil
}



func stringPtr(s string) *string { return &s }
func intPtr(i int) *int { return &i }
func timePtr(t time.Time) *time.Time { return &t }

func getStringValue(ptr *string) string {
    if ptr == nil { return "" }
    return *ptr
}

func getIntValue(ptr *int) int {
    if ptr == nil { return 0 }
    return *ptr
}

func timeToTimestamp(t time.Time) *timestamppb.Timestamp {
    return timestamppb.New(t)
}

func timeToTimestampPtr(t *time.Time) *timestamppb.Timestamp {
    if t == nil { return nil }
    return timestamppb.New(*t)
}

func connectDatabase() *gorm.DB {
	dsn := "root:admin@tcp(127.0.0.1:3306)/crawler_db?charset=utf8mb4&parseTime=True&loc=Local"
    db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
    if err != nil {
        slog.Error("failed to connect database")
    }
    return db
}

func auto_migrate(db *gorm.DB) error {
    err := db.AutoMigrate(&Page{})
    if err != nil {
        slog.Error("Migration failed","Error",err)
        return fmt.Errorf("migration failed: %w", err)
    }
    return nil
}

func main() {
    db := connectDatabase()
    logger := slog.New(slog.NewJSONHandler(os.Stdout,
	&slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)
    if err := auto_migrate(db); err != nil {
        slog.Error("Migration failed","Error", err)
    }
    
    
    server := &Storage_Server{db: db}
    
    lis, err := net.Listen("tcp", ":50053")
    if err != nil {
        slog.Error("Failed to listen", "Error",err)
    }
    
    grpcServer := grpc.NewServer()
    pb.RegisterStorageServiceServer(grpcServer, server)
    stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-stop
		slog.Info("Received shutdown signal...")
		slog.Info("Gracefully stopping gRPC server...")
		grpcServer.GracefulStop()
		slog.Info("gRPC server stopped")
	}()

    slog.Info("Storage Service is running on port 50053")
    if err := grpcServer.Serve(lis); err != nil {
        slog.Error("Failed to serve","Error", err)
    }
}
