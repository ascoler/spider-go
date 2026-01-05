package main

import (
	"context"
	"encoding/json"
	"fmt"
	pb "local/crawler/gen/crawler"
	queue "local/crawler/gen/queue"
	storage "local/crawler/gen/storage"
	"log"
	"net"
	"net/http"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"os/signal"
	"syscall"

	"github.com/PuerkitoBio/goquery"
	lru "github.com/hashicorp/golang-lru/v2"
	boom "github.com/tylertreat/BoomFilters"
	"google.golang.org/grpc"
)
type CachedPage struct{
	Title string
	Content string
}
type CrawlerServer struct {
	pb.UnimplementedCrawlerServiceServer
	queueClient   queue.QueueServiceClient
	storageClient storage.StorageServiceClient
	config        Config
	urlCache      *lru.Cache[string, *CachedPage]
	filter        *boom.BloomFilter
}

func NewCrawlerServer(
	queueClient queue.QueueServiceClient,
	storageClient storage.StorageServiceClient,
	config Config,

) *CrawlerServer {
	cache, err := lru.New[string, *CachedPage](50000)
	if err != nil {
		panic(err)
	}
	filter := boom.NewBloomFilter(100000, 0.01)
	return &CrawlerServer{
		queueClient:   queueClient,
		storageClient: storageClient,
		config:        config,
		urlCache:      cache,
		filter:        filter,
	}
}

type CrawlState struct {
	mu        sync.RWMutex
	visited   map[string]bool
	processed int
	maxpages  int
}

func (s *CrawlState) ShouldProcess(url string) bool {
	s.mu.RLock()
	if s.processed >= s.maxpages {
		s.mu.RUnlock()
		return false
	}
	s.mu.RUnlock()

	s.mu.RLock()
	if s.visited[url] {
		s.mu.RUnlock()
		return false
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	s.visited[url] = true
	s.processed++
	return true

}

type Config struct {
	MAX_DEPTH        int    `json:"max_depth"`
	MAX_PAGES        int    `json:"max_pages"`
	WORKER_POOL_SIZE int    `json:"worker_pool_size"`
	REQUEST_TIMEOUT  int    `json:"request_timeout"`
	MAX_RETRIES      int    `json:"max_retries"`
	RETRY_DELAY      int    `json:"retry_delay"`
	RATE_LIMIT_DELAY int    `json:"rate_limit_delay"`
	STORAGE_TYPE     string `json:"storage_type"`
	LOG_LEVEL        string `json:"log_level"`
	OUTPUT_FILE      string `json:"output_file"`
}






func Get_Config() (Config, error) {
	file, err := os.Open("/home/wake_up/myproj/spider-go/config/config.json")
	if err != nil {
		slog.Error("failed to open config file", "error", err)
	}
	defer file.Close()

	var config Config
	err = json.NewDecoder(file).Decode(&config)
	if err != nil {
		slog.Error("Failed to decode json","error",err)
	}
	return config, nil

}
func (s *CrawlerServer) CheckPageExists(ctx context.Context, url string) (string,string, error) {

	if cached, ok := s.urlCache.Get(url); ok {
		slog.Info("Entry found in cache")
		return cached.Content,cached.Title,nil
	}


	if !s.filter.Test([]byte(url)) {
		return "", "",nil
	}

	
	

	se, err := s.storageClient.GetPage(ctx, &storage.GetPageRequest{
			Url: url,
		})
		if err != nil {
			log.Printf("Error %v", err)
		}
	if err != nil {
		return "","",nil
	}

	
	s.urlCache.Add(url, &CachedPage{
			Content: se.Page.ContentText,
			Title: se.Page.Title,})
		
	s.filter.Add([]byte(url))
	

	return se.Page.ContentText,se.Page.Title,nil
}
func ResolveLink(url string) bool {
	return strings.HasPrefix(url, "http://") ||
		strings.HasPrefix(url, "https://")
}
func (s *CrawlerServer) TakeJobs(ctx context.Context, jobs chan<- string, queueName string) (string, error) {

	sizeResp, err := s.queueClient.GetQueueSize(ctx, &queue.QueueSizeRequest{
		QueueName: queueName,
	})
	if err != nil {
		slog.Error("Error get size queue","Error",err)
		return "", fmt.Errorf("ошибка получения размера очереди: %v", err)
	}

	if sizeResp.Size == 0 {
		slog.Info("Queue is empty")
		return "Очередь пуста", nil
	}

	jobsProcessed := 0
	for i := 0; i < int(sizeResp.Size); i++ {

		if ctx.Err() != nil {
			return fmt.Sprintf("Обработано %d задач перед отменой", jobsProcessed), nil
		}

		popResp, err := s.queueClient.Pop(ctx, &queue.PopRequest{
			QueueName: queueName,
		})
		if err != nil {

			slog.Error("Error dequeuing","Error",err)
			break
		}

		for _, url := range popResp.Urls {
			select {
			case jobs <- url:
				jobsProcessed++
				slog.Info(" New task append", "url",url)
			case <-ctx.Done():
				return fmt.Sprintf("Обработано %d задач перед отменой", jobsProcessed), nil
			default:

				select {
				case jobs <- url:
					jobsProcessed++
					slog.Info(" New task append", "url",url)
				case <-ctx.Done():
					return fmt.Sprintf("Обработано %d задач перед отменой", jobsProcessed), nil
				case <-time.After(100 * time.Millisecond):
					slog.Info(" Channel jobs crowded,skip", "url",url)
				}
			}
		}
	}

	return fmt.Sprintf("Успешно обработано %d задач из очереди", jobsProcessed), nil
}
func (s *CrawlerServer) push_work(ctx context.Context, queue_name string, urls []string) error {
	_, err := s.queueClient.Push(ctx, &queue.PushRequest{
		QueueName: queue_name,
		Urls:      urls,
	})
	return err

}
func (s *CrawlerServer) CreateWorker(ctx context.Context, ALLcontent chan<- string, jobs <-chan string, result chan<- string, newLinks chan<- string) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("Worker recovered from panic","Error",r)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case url, ok := <-jobs:
			if !ok {
				return
			}

			func() {
				defer func() {
					if r := recover(); r != nil {
						slog.Error("Worker panic while processing","Url",url,"Error", r)
					}
				}()

				links, title, content := s.AnalysisLink(ctx, url, 3)
				select {
				case result <- fmt.Sprintf("✅ Worker %s done: %d links, title: %s", url, len(links), title):
					slog.Info(" Worker done","Count links",len(links),"Title",title)
				case <-ctx.Done():
					return
				}

				for _, link := range links {
					select {
					case newLinks <- link:
					case <-ctx.Done():
						return
					}
				}
				if len(content) > 100 {
					select {
					case ALLcontent <- content:
					case <-ctx.Done():
						return
					}
				}
			}()
		}
	}
}

func (s *CrawlerServer) AnalysisLink(ctx context.Context, url string, depth int) ([]string, string, string) {
	if depth <= 0 {
		return []string{}, "", ""
	}
	if s.storageClient == nil {
		slog.Info(" Storage client is nil for URL","Url",url)
		return []string{}, "", ""
	}
	contents, titles, err := s.CheckPageExists(ctx, url)
    if err == nil && contents != "" {
        
        slog.Info(" Page already exists (from cache/storage)", "Url",url)
        return []string{}, titles, contents
    }
    
    
    
	res, err := http.Get(url)
	var links []string
	var titleBuilder, contentBuilder strings.Builder

	if err != nil {
		slog.Info("Failed to connect to the target page", "Url",url,"Error",err)
		return links, "", ""
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		slog.Warn("HTTP Error", slog.String("Status code", res.Status))
		return links, "", ""
	}

	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		slog.Warn("Failed to parse the target page", "Url",url,"Error",err)
		return links, "", ""
	}

	slog.Info("\nLinks found: %s\n","Url",url)

	doc.Find("a").Each(func(i int, sel *goquery.Selection) {
		href, exists := sel.Attr("href")

		if !ResolveLink(href) {
			baseURL := strings.TrimSuffix(url, "/")
			href = baseURL + "/" + strings.TrimPrefix(href, "/")
		}

		if exists && href != "" {
			text := strings.TrimSpace(sel.Text())
			slog.Info("Link","Href", href,"Text", text)
			links = append(links, href)
		}
	})

	doc.Find("title").Each(func(i int, sel *goquery.Selection) {
		title := strings.TrimSpace(sel.Text())
		titleBuilder.WriteString(title)
		slog.Info("Title","Title",title)
	})

	doc.Find("p, h1, h2, h3, h4, h5, h6, li, blockquote, figcaption, dd, dt").Each(func(i int, sel *goquery.Selection) {
		text := strings.TrimSpace(sel.Text())
		if text != "" {
			contentBuilder.WriteString(text)
			contentBuilder.WriteString(" ")
		}
	})

	doc.Find("img").Each(func(i int, sel *goquery.Selection) {
		src, exists := sel.Attr("src")
		if !ResolveLink(src) {
			src = url + src
		}
		if exists {
			alt, _ := sel.Attr("alt")
			slog.Info("Picture","Src", src,"Alt", alt)
		}
	})

	var metaContent strings.Builder
	metaTags := []string{"keywords", "robots", "viewport", "author", "description"}
	for _, tag := range metaTags {
		content, _ := doc.Find(fmt.Sprintf("meta[name='%s']", tag)).Attr("content")
		if content != "" {
			metaContent.WriteString(tag)
			metaContent.WriteString(": ")
			metaContent.WriteString(content)
			metaContent.WriteString("; ")
		}
	}

	title := titleBuilder.String()
	content := strings.TrimSpace(contentBuilder.String())

	_, err = s.storageClient.SavePage(ctx, &storage.SavePageRequest{
		Page: &storage.Page{
			Url:         url,
			StatusCode:  int32(res.StatusCode),
			Title:       title,
			ContentText: content,
			Depth:       int32(depth),
			IsProcessed: true,
		},
	})

	if err != nil {
		slog.Error("Failed to save page to storage", "Error",err)
	} else {
		s.filter.Add([]byte(url))
		s.urlCache.Add(url,&CachedPage{
			Title: title,
			Content: content,
		})
		slog.Info("Page saved to storage: %s", "Url",url)
	}
	
	slog.Info("Content found","Title",title,"Content",content)
	slog.Any("Links",links)
	return links, title, content
}

func (s *CrawlerServer) StartCrawling(parentCtx context.Context, req *pb.StartCrawlingRequest) (*pb.StartCrawlingResponse, error) {
	config, err := Get_Config()
	if err != nil {
		slog.Error("Cannot open config","Error",err)
	}

	ctx, cancel := context.WithCancel(parentCtx)
	var wg sync.WaitGroup
	defer cancel()

	maxPages := config.MAX_PAGES
	if req.MaxPages > 0 {
		maxPages = int(req.MaxPages)
	}

	jobs := make(chan string, 5000)
	results := make(chan string, 1000)
	newLinks := make(chan string, 10000)
	ALLcontent := make(chan string, 10000)

	if len(req.SeedUrls) > 0 {

		s.queueClient.ClearQueue(ctx, &queue.ClearQueueRequest{
			QueueName: "crawl_queue",
		})

		err := s.push_work(ctx, "crawl_queue", req.SeedUrls)
		if err != nil {
			slog.Warn("Failed to add seed URLs", "Error",err)
		} else {
			slog.Info("Added seed URLs to queue", "Count_links",len(req.SeedUrls))
		}
	}

	for w := 1; w <= config.WORKER_POOL_SIZE; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.CreateWorker(ctx, ALLcontent, jobs, results, newLinks)
		}()
	}

	go func() {
		for result := range results {
			slog.Info("Result","All",result)
		}
	}()

	var allContent []string
	var contentMutex sync.Mutex

	go func() {
		for content := range ALLcontent {
			contentMutex.Lock()
			allContent = append(allContent, content)
			contentMutex.Unlock()
			if len(content) > 100 {
				slog.Info("content received","Content",content[:100])
			}
		}
	}()

	state := &CrawlState{
		visited:  make(map[string]bool),
		maxpages: maxPages,
	}

	done := make(chan bool)

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		idleCounter := 0
		maxIdleCount := 5

		for {
			select {
			case <-ctx.Done():
				slog.Info("Context cancelled")
				close(done)
				return

			case <-ticker.C:
				message, err := s.TakeJobs(ctx, jobs, "crawl_queue")
				if err != nil {
					slog.Error("Error TakeJobs","Error",err)
				} else if message != "Очередь пуста" {
					slog.Info("TakeJobs", "Info",message)
					idleCounter = 0
				} else {
					idleCounter++
				}

			case link := <-newLinks:
				idleCounter = 0

				if state.ShouldProcess(link) {
					err := s.push_work(ctx, "crawl_queue", []string{link})
					if err != nil {
						slog.Error("Error push_work: %v", "Error",err)
					} else {
						slog.Info("Added page","State", state.processed,"MaxPages", maxPages,"Url", link)
					}
				}

				if state.processed >= maxPages {
					slog.Info("The limit of pages has been reached", "Max_Pages",maxPages)
					time.Sleep(3 * time.Second)
					cancel()
					close(done)
					return
				}
			}

			if idleCounter >= maxIdleCount {
				slog.Info("Завершение по бездействию")
				cancel()
				close(done)
				return
			}
		}
	}()

	<-done

	close(jobs)
	wg.Wait()
	close(results)
	close(newLinks)
	close(ALLcontent)
		
	return &pb.StartCrawlingResponse{
		Content: allContent,
		Status:  fmt.Sprintf("Completed: %d pages", state.processed),
	}, nil
}
func main() {
	queueConn, err := grpc.Dial("localhost:50052", grpc.WithInsecure())
	logger := slog.New(slog.NewJSONHandler(os.Stdout,
	&slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)
	if err != nil {
		slog.Error("Failed to connect to queue", "Error",err)
	}
	defer queueConn.Close()

	storageConn, err := grpc.Dial("localhost:50053", grpc.WithInsecure())
	if err != nil {
		slog.Error("Failed to connect to storage:", "Error",err)
	}
	defer storageConn.Close()

	config, err := Get_Config()
	if err != nil {
		slog.Error("Failed to load config", "Error",err)
	}

	crawlerServer := NewCrawlerServer(
		queue.NewQueueServiceClient(queueConn),
		storage.NewStorageServiceClient(storageConn),
		config,
	)

	if crawlerServer.storageClient == nil {
		slog.Error("Storage client is nil!")
	}
	slog.Info("Storage client initialized")

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		slog.Error("Failed to listen", "Error",err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterCrawlerServiceServer(grpcServer, crawlerServer)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-stop
		slog.Info("Received shutdown signal...")
		slog.Info("Gracefully stopping gRPC server...")
		grpcServer.GracefulStop()
		slog.Info("gRPC server stopped")
	}()

	slog.Info("gRPC Server starting on port 50051...")
	if err := grpcServer.Serve(lis); err != nil {
		slog.Error("Server failed", "Error",err)
	}
}
