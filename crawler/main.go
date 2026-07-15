package main

import (
	"context"
	"encoding/json"
	"fmt"
	pb "local/crawler/gen/crawler"
	queue "local/crawler/gen/queue"
	storage "local/crawler/gen/storage"
	
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
	"net/url"

	"os/signal"
	"syscall"
	"github.com/temoto/robotstxt"
	"github.com/PuerkitoBio/goquery"
	lru "github.com/hashicorp/golang-lru/v2"
	boom "github.com/tylertreat/BoomFilters"
	"google.golang.org/grpc"
)

type CachedPage struct {
	Title   string
	Content string
}
type CrawlerServer struct {
	pb.UnimplementedCrawlerServiceServer
	queueClient   queue.QueueServiceClient
	storageClient storage.StorageServiceClient
	config        Config
	robotCahche *lru.Cache[string, *robotstxt.Group]
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

type LinkTask struct {
	URL   string `json:"url"`
	Depth int    `json:"depth"`
}

func encodeTask(t LinkTask) (string, error) {
	b, err := json.Marshal(t)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func decodeTask(s string) (LinkTask, error) {
	var t LinkTask
	err := json.Unmarshal([]byte(s), &t)
	return t, err
}

type CrawlState struct {
	mu        sync.RWMutex
	visited   map[string]bool
	
	processed int
	maxpages  int
}

func (s *CrawlState) ShouldProcess(url string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.processed >= s.maxpages {
		return false
	}
	if s.visited[url] {
		return false
	}
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
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/home/wakeup/spider-go/config/config.json"
	}

	file, err := os.Open(path)
	if err != nil {
		slog.Error("failed to open config file", "error", err)
		return Config{}, err
	}
	defer file.Close()

	var config Config
	err = json.NewDecoder(file).Decode(&config)
	if err != nil {
		slog.Error("Failed to decode json", "error", err)
		return Config{}, err
	}
	return config, nil
}

func (s *CrawlerServer) CheckPageExists(ctx context.Context, url string) (string, string, error) {

	if cached, ok := s.urlCache.Get(url); ok {
		slog.Info("Entry found in cache")
		return cached.Content, cached.Title, nil
	}

	if !s.filter.Test([]byte(url)) {
		return "", "", nil
	}

	se, err := s.storageClient.GetPage(ctx, &storage.GetPageRequest{
		Url: url,
	})

	if err != nil {
		slog.Error("Error fetching page", "Url", url, "Error", err)
		return "", "", err
	}

	s.urlCache.Add(url, &CachedPage{
		Content: se.Page.ContentText,
		Title:   se.Page.Title})

	s.filter.Add([]byte(url))

	return se.Page.ContentText, se.Page.Title, nil
}

func ResolveLink(url string) bool {
	return strings.HasPrefix(url, "http://") ||
		strings.HasPrefix(url, "https://")
}


func (s *CrawlerServer) pushLinks(ctx context.Context, queueName string, tasks []LinkTask) error {
	encoded := make([]string, 0, len(tasks))
	for _, t := range tasks {
		enc, err := encodeTask(t)
		if err != nil {
			slog.Error("Failed to encode task", "Url", t.URL, "Error", err)
			continue
		}
		encoded = append(encoded, enc)
	}
	if len(encoded) == 0 {
		return nil
	}
	return s.push_work(ctx, queueName, encoded)
}

func (s *CrawlerServer) TakeJobs(ctx context.Context, jobs chan<- LinkTask, queueName string) (string, error) {

	sizeResp, err := s.queueClient.GetQueueSize(ctx, &queue.QueueSizeRequest{
		QueueName: queueName,
	})
	if err != nil {
		slog.Error("Error get size queue", "Error", err)
		return "", fmt.Errorf("error get size queue: %v", err)
	}

	if sizeResp.Size == 0 {
		slog.Info("Queue is empty")
		return "Queue is empty", nil
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
			slog.Error("Error dequeuing", "Error", err)
			return "", err
		}

		for _, raw := range popResp.Urls {
			task, err := decodeTask(raw)
			if err != nil {
				slog.Error("Failed to decode task, skipping", "raw", raw, "Error", err)
				continue
			}

			if task.Depth <= 0 {
				slog.Info("Skipping task, depth exhausted", "Url", task.URL)
				continue
			}

			select {
			case jobs <- task:
				jobsProcessed++
				slog.Info(" New task append", "url", task.URL, "depth", task.Depth)
			case <-ctx.Done():
				return fmt.Sprintf("Обработано %d задач перед отменой", jobsProcessed), nil
			default:
				select {
				case jobs <- task:
					jobsProcessed++
					slog.Info(" New task append", "url", task.URL, "depth", task.Depth)
				case <-ctx.Done():
					return fmt.Sprintf("Обработано %d задач перед отменой", jobsProcessed), nil
				case <-time.After(100 * time.Millisecond):
					slog.Info(" Channel jobs crowded,skip", "url", task.URL)
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

func (s *CrawlerServer) CreateWorker(ctx context.Context, ALLcontent chan<- string, jobs <-chan LinkTask, result chan<- string, newLinks chan<- LinkTask) {

	for {
		select {
		case <-ctx.Done():
			return
		case task, ok := <-jobs:
			if !ok {
				return
			}

			func() {
				defer func() {
					if r := recover(); r != nil {
						slog.Error("Worker panic while processing", "Url", task.URL, "Error", r)
					}
				}()

				links, title, content := s.AnalysisLink(ctx, task.URL, task.Depth)
				select {
				case result <- fmt.Sprintf("✅ Worker %s done: %d links, title: %s", task.URL, len(links), title):
					slog.Info(" Worker done", "Count links", len(links), "Title", title)
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


func (s *CrawlerServer) AnalysisLink(ctx context.Context, urls string, depth int) ([]LinkTask, string, string) {
	path,err := url.Parse(urls)
	if err != nil {
		slog.Error("Failed to parse URL", "Url", urls, "Error", err)
		return []LinkTask{}, "", ""
	}
	host := path.Hostname()
	if s.robotCahche.Contains(host) {
		robotsData, _ := s.robotCahche.Get(host)
		if !robotsData.Test(path.Path) {
			slog.Info("Access denied by robots.txt", "Url", urls)
			return []LinkTask{}, "", ""
		}
	}else {
    robotsData, err := getRobotsTxt(path.Scheme + "://" + host + "/robots.txt")

    if err != nil {
        slog.Error("Failed to get robots.txt", "Url", urls, "Error", err)
    } else {
        s.robotCahche.Add(host, robotsData) // кладём в кэш только если реально получили
        if !robotsData.Test(path.Path) {
            slog.Info("Access denied by robots.txt", "Url", urls)
            return []LinkTask{}, "", ""
        }
    }
}
	
		
	

	
	
	
	
	
	if depth <= 0 {
		return []LinkTask{}, "", ""
	}
	if s.storageClient == nil {
		slog.Info(" Storage client is nil for URL", "Url", urls)
		return []LinkTask{}, "", ""
	}
	contents, titles, err := s.CheckPageExists(ctx, urls)
	if err == nil && contents != "" {
		slog.Info(" Page already exists (from cache/storage)", "Url", urls)
		return []LinkTask{}, titles, contents
	}

	res, err := http.Get(urls)
	var links []LinkTask
	var titleBuilder, contentBuilder strings.Builder

	if err != nil {
		slog.Info("Failed to connect to the target page", "Url", urls, "Error", err)
		return links, "", ""
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		slog.Warn("HTTP Error", slog.String("Status code", res.Status))
		return links, "", ""
	}

	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		slog.Warn("Failed to parse the target page", "Url", urls, "Error", err)
		return links, "", ""
	}

	slog.Info("Links found", "Url", urls)

	doc.Find("a").Each(func(i int, sel *goquery.Selection) {
		href, exists := sel.Attr("href")

		if !ResolveLink(href) {
			baseURL := strings.TrimSuffix(urls, "/")
			href = baseURL + "/" + strings.TrimPrefix(href, "/")
		}

		if exists && href != "" {
			text := strings.TrimSpace(sel.Text())
			slog.Info("Link", "Href", href, "Text", text)
			links = append(links, LinkTask{URL: href, Depth: depth - 1})
		}
	})

	doc.Find("title").Each(func(i int, sel *goquery.Selection) {
		title := strings.TrimSpace(sel.Text())
		titleBuilder.WriteString(title)
		slog.Info("Title", "Title", title)
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
			src = urls + src
		}
		if exists {
			alt, _ := sel.Attr("alt")
			slog.Info("Picture", "Src", src, "Alt", alt)
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
			Url:         urls,
			StatusCode:  int32(res.StatusCode),
			Title:       title,
			ContentText: content,
			Depth:       int32(depth),
			IsProcessed: true,
		},
	})

	if err != nil {
		slog.Error("Failed to save page to storage", "Error", err)
		return links, title, content
	}

	s.filter.Add([]byte(urls))
	s.urlCache.Add(urls, &CachedPage{
		Title:   title,
		Content: content,
	})
	slog.Info("Page saved to storage", "Url", urls)

	return links, title, content
}

func (s *CrawlerServer) StartCrawling(parentCtx context.Context, req *pb.StartCrawlingRequest) (*pb.StartCrawlingResponse, error) {
	config, err := Get_Config()
	if err != nil {
		slog.Error("Cannot open config", "Error", err)
		return nil, err
	}

	ctx, cancel := context.WithCancel(parentCtx)
	var wg sync.WaitGroup
	defer cancel()

	maxPages := config.MAX_PAGES
	if req.MaxPages > 0 {
		maxPages = int(req.MaxPages)
	}
	if maxPages <= 0 {
		return nil, fmt.Errorf("invalid max_pages: must be greater than 0, got %d", maxPages)
	}

	startDepth := config.MAX_DEPTH
	if startDepth <= 0 {
		return nil, fmt.Errorf("invalid max_depth in config: must be greater than 0, got %d", startDepth)
	}

	jobs := make(chan LinkTask, 5000)
	results := make(chan string, 1000)
	newLinks := make(chan LinkTask, 10000)
	ALLcontent := make(chan string, 10000)

	if len(req.SeedUrls) > 0 {
		s.queueClient.ClearQueue(ctx, &queue.ClearQueueRequest{
			QueueName: "crawl_queue",
		})

		seedTasks := make([]LinkTask, 0, len(req.SeedUrls))
		for _, u := range req.SeedUrls {
			seedTasks = append(seedTasks, LinkTask{URL: u, Depth: startDepth})
		}

		if err := s.pushLinks(ctx, "crawl_queue", seedTasks); err != nil {
			slog.Warn("Failed to add seed URLs", "Error", err)
			return nil, fmt.Errorf("failed to add seed URLs: %v", err)
		}
		slog.Info("Added seed URLs to queue", "Count_links", len(req.SeedUrls), "Depth", startDepth)
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
			slog.Info("Result", "All", result)
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
				slog.Info("content received", "Content", content[:100])
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
					slog.Error("Error TakeJobs", "Error", err)
					idleCounter++
					continue
				} else if message != "Queue is empty" {
					slog.Info("TakeJobs", "Info", message)
					idleCounter = 0
				} else {
					idleCounter++
				}

			case task := <-newLinks:
				idleCounter = 0

				if task.Depth > 0 && state.ShouldProcess(task.URL) {
					err := s.pushLinks(ctx, "crawl_queue", []LinkTask{task})
					if err != nil {
						for attempt := 1; attempt <= config.MAX_RETRIES; attempt++ {
							slog.Warn("Retrying push_work", "Attempt", attempt, "Url", task.URL)

							select {
							case <-time.After(time.Duration(config.RETRY_DELAY) * time.Second):
							case <-ctx.Done():
								return
							}

							err = s.pushLinks(ctx, "crawl_queue", []LinkTask{task})
							if err == nil {
								slog.Info("Successfully pushed link after retry", "Url", task.URL)
								break
							}
						}
						if err != nil {
							slog.Error("Error push_work after retries", "Error", err, "Url", task.URL)
						}
					} else {
						slog.Info("Added page", "State", state.processed, "MaxPages", maxPages, "Url", task.URL, "Depth", task.Depth)
					}
				}

				if state.processed >= maxPages {
					slog.Info("The limit of pages has been reached", "Max_Pages", maxPages)
					time.Sleep(3 * time.Second)
					cancel()
					close(done)
					return
				}
			}

			if idleCounter >= maxIdleCount {
				slog.Info("Completion due to inactivity")
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
	logger := slog.New(slog.NewJSONHandler(os.Stdout,
		&slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)

	queueConn, err := grpc.Dial("localhost:50052", grpc.WithInsecure())
	if err != nil {
		slog.Error("Failed to connect to queue", "Error", err)
		os.Exit(1)
	}
	defer queueConn.Close()

	storageConn, err := grpc.Dial("localhost:50053", grpc.WithInsecure())
	if err != nil {
		slog.Error("Failed to connect to storage:", "Error", err)
		os.Exit(1)
	}
	defer storageConn.Close()

	config, err := Get_Config()
	if err != nil {
		slog.Error("Failed to load config", "Error", err)
		os.Exit(1)
	}

	crawlerServer := NewCrawlerServer(
		queue.NewQueueServiceClient(queueConn),
		storage.NewStorageServiceClient(storageConn),
		config,
	)

	if crawlerServer.storageClient == nil {
		slog.Error("Storage client is nil!")
		os.Exit(1)
	}
	slog.Info("Storage client initialized")

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		slog.Error("Failed to listen", "Error", err)
		os.Exit(1)
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
		slog.Error("Server failed", "Error", err)
		os.Exit(1)
	}
}