package main

import (
	"context"
	"encoding/json"
	"fmt"
	pb "local/crawler/internal/gen/crawler"
	queue "local/crawler/internal/gen/queue"
	storage "local/crawler/internal/gen/storage"
	"log"
	"net"
	"net/http"
	
	"os"
	"strings"
	"sync"
	"time"
    
    "os/signal"
    "syscall"
	"github.com/PuerkitoBio/goquery"
	"github.com/hashicorp/golang-lru/v2"
	"github.com/tylertreat/BoomFilters"
	"google.golang.org/grpc"
)
type CrawlerServer struct {
    pb.UnimplementedCrawlerServiceServer
    queueClient   queue.QueueServiceClient
    storageClient storage.StorageServiceClient
    config        Config
    urlCache    *lru.Cache[string,string]
    filter *boom.BloomFilter
    
}


func NewCrawlerServer(
    queueClient queue.QueueServiceClient,
    storageClient storage.StorageServiceClient,
    config Config,
  

   
) *CrawlerServer {
    cache, err := lru.New[string, string](50000)
    if err != nil {
        panic(err)
    }
    filter := boom.NewBloomFilter(100000, 0.01)
    return &CrawlerServer{
        queueClient:   queueClient,
        storageClient: storageClient,
        config:        config,
        urlCache: cache,
        filter:  filter,
        
    }
}

type CrawlState struct {
            mu sync.RWMutex
            visited map[string]bool
            processed int
            maxpages int
            
        }
func(s *CrawlState) ShouldProcess(url string) bool{
       s.mu.RLock()
        if s.processed >= s.maxpages {
        s.mu.RUnlock()
        return false
        }
        s.mu.RUnlock()
    
    
       s.mu.RLock()
        if s.visited[url]{
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






type Config struct{
	MAX_DEPTH int `json:"max_depth"`
    MAX_PAGES int `json:"max_pages"`
    WORKER_POOL_SIZE int `json:"worker_pool_size"`
    REQUEST_TIMEOUT int  `json:"request_timeout"`
    MAX_RETRIES int `json:"max_retries"`
    RETRY_DELAY int `json:"retry_delay"`
    RATE_LIMIT_DELAY int `json:"rate_limit_delay"`
    STORAGE_TYPE  string `json:"storage_type"`
    LOG_LEVEL string `json:"log_level"`
    OUTPUT_FILE string `json:"output_file"`
}
func Get_Config() (Config,error){
	file,err := os.Open("/home/wake_up/on_github/internal/config/config.json")
	if err != nil {
		log.Fatal("Failed to open config file", err)
	}
	defer file.Close()
	
    var config Config
    err = json.NewDecoder(file).Decode(&config)
	if err != nil {
    	log.Fatal(err)
	}
	return config,nil
	

}
func (s *CrawlerServer) CheckPageExists(ctx context.Context, url string) (bool, error) {
  
   

    
    if !s.filter.Test([]byte(url)) {
        return false, nil
    }

  
    if _, ok := s.urlCache.Get(url); ok {
        return true, nil
    }

 
    check, err := s.storageClient.CheckPageExists(ctx, &storage.CheckPageRequest{
        Url: url,
    })
    if err != nil {
        return false, fmt.Errorf("storage error: %v", err)
    }

    
    if check.Exists {
        s.urlCache.Add(url, check.PageId)
        
        s.filter.Add([]byte(url))
    }

    return check.Exists, nil
}
func ResolveLink(url string) bool{
	return strings.HasPrefix(url,"http://") || 
	strings.HasPrefix(url,"https://")
}
func (s *CrawlerServer) TakeJobs(ctx context.Context, jobs chan<- string, queueName string) (string, error) {
   
    sizeResp, err := s.queueClient.GetQueueSize(ctx, &queue.QueueSizeRequest{
        QueueName: queueName,
    })
    if err != nil {
        return "", fmt.Errorf("ошибка получения размера очереди: %v", err)
    }
    
    
    if sizeResp.Size == 0 {
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
          
            log.Printf("Ошибка при извлечении из очереди: %v", err)
            break
        }
        
        
        for _, url := range popResp.Urls {
            select {
            case jobs <- url:
                jobsProcessed++
                log.Printf("✅ Добавлена задача: %s", url)
            case <-ctx.Done():
                return fmt.Sprintf("Обработано %d задач перед отменой", jobsProcessed), nil
            default:
               
                select {
                case jobs <- url:
                    jobsProcessed++
                    log.Printf("✅ Добавлена задача: %s", url)
                case <-ctx.Done():
                    return fmt.Sprintf("Обработано %d задач перед отменой", jobsProcessed), nil
                case <-time.After(100 * time.Millisecond):
                    log.Printf("⚠️ Канал jobs переполнен, пропускаем задачу: %s", url)
                }
            }
        }
    }
    
    return fmt.Sprintf("Успешно обработано %d задач из очереди", jobsProcessed), nil
}
func (s *CrawlerServer) push_work(ctx context.Context,queue_name string,urls []string)(error){
	_,err := s.queueClient.Push(ctx, &queue.PushRequest{
		QueueName: queue_name,
		Urls: urls,
	})
	return err
	


}
func (s *CrawlerServer) CreateWorker(ctx context.Context, ALLcontent chan <- string, jobs <-chan string, result chan<- string, newLinks chan<- string){
    defer func() {
        if r := recover(); r != nil {
            log.Printf("⚠️ Worker recovered from panic: %v", r)
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
                        log.Printf("❌ Worker panic while processing %s: %v", url, r)
                    }
                }()
                
                links, title, content := s.AnalysisLink(ctx, url, 3)
                select {
                case result <- fmt.Sprintf("✅ Worker %s done: %d links, title: %s", url, len(links), title):
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
        log.Printf("❌ Storage client is nil for URL: %s", url)
        return []string{}, "", ""
    }
    exists,err := s.CheckPageExists(ctx,url)
    if err != nil{
        log.Printf("Error %v",err)

    }else if exists {
        se,err := s.storageClient.GetPage(ctx,&storage.GetPageRequest{
            Url: url,

        })
        if err != nil{
            log.Printf("Error %v",err)
        }
        return  []string{},se.Page.Title,se.Page.ContentText
    }
	res, err := http.Get(url)
	var links []string
	var titleBuilder, contentBuilder strings.Builder
	
	if err != nil {
		log.Printf("Failed to connect to the target page %s: %v", url, err)
		return links, "", ""
	}
	defer res.Body.Close()
	
	if res.StatusCode != 200 {
		log.Printf("HTTP Error %d: %s", res.StatusCode, res.Status)
		return links, "", ""
	}
	
	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		log.Printf("Failed to parse the target page %s: %v", url, err)
		return links, "", ""
	}
	
	fmt.Printf("\nLinks found: %s\n", url)
	
	
	doc.Find("a").Each(func(i int, sel *goquery.Selection) {
		href, exists := sel.Attr("href")
		
		if !ResolveLink(href) {
			baseURL := strings.TrimSuffix(url, "/")
			href = baseURL + "/" + strings.TrimPrefix(href, "/")
		}
	
		if exists && href != "" {
			text := strings.TrimSpace(sel.Text())
			fmt.Printf("Ссылка %d: %s - %s\n", i+1, href, text)
			links = append(links, href)
		}
	})
	
	
	doc.Find("title").Each(func(i int, sel *goquery.Selection) {
		title := strings.TrimSpace(sel.Text())
		titleBuilder.WriteString(title)
		fmt.Printf("Заголовок: %s\n", title)
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
			fmt.Printf("Изображение %d: %s - %s\n", i+1, src, alt)
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
		log.Printf("Failed to save page to storage: %v", err)
	} else {
        s.filter.Add([]byte(url))
        s.urlCache.Add(url,content)
		log.Printf("✅ Page saved to storage: %s", url)
	}
	
	return links, title, content
}

func (s *CrawlerServer) StartCrawling(parentCtx context.Context, req *pb.StartCrawlingRequest) (*pb.StartCrawlingResponse, error) {
    config, err := Get_Config()
    if err != nil {
        log.Fatal("Error")
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
    ALLcontent := make(chan string,10000)

    if len(req.SeedUrls) > 0 {
        err := s.push_work(ctx, "crawl_queue", req.SeedUrls)
        if err != nil {
            log.Printf("❌ Failed to add seed URLs: %v", err)
        } else {
            log.Printf("✅ Added %d seed URLs to queue", len(req.SeedUrls))
        }
    }

  
    for w := 1; w <= config.WORKER_POOL_SIZE; w++ {
        wg.Add(1)
        go func(){
            defer wg.Done()
            s.CreateWorker(ctx, ALLcontent, jobs, results, newLinks)
        }()
    }

    go func() {
        for result := range results {
            log.Println(result)
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
                log.Printf("📄 Получен контент: %s...", content[:100])
            }
        }
    }()
    
    state := &CrawlState{
        visited: make(map[string]bool),
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
                log.Println("Context cancelled")
                close(done)
                return
                
            case <-ticker.C:
                message, err := s.TakeJobs(ctx, jobs, "crawl_queue")
                if err != nil {
                    log.Printf("Ошибка TakeJobs: %v", err)
                } else if message != "Очередь пуста" {
                    log.Printf("TakeJobs: %s", message)
                    idleCounter = 0
                } else {
                    idleCounter++
                }
                
            case link := <-newLinks:
                idleCounter = 0
                
                if state.ShouldProcess(link) {
                    err := s.push_work(ctx, "crawl_queue", []string{link})
                    if err != nil {
                        log.Printf("Ошибка push_work: %v", err)
                    } else {
                        log.Printf("📄 Добавлена страница %d/%d: %s", state.processed, maxPages, link)
                    }
                }
                
                if state.processed >= maxPages {
                    log.Printf("✅ Достигнут лимит в %d страниц", maxPages)
                    cancel()
                    close(done)
                    return
                }
            }
            
            if idleCounter >= maxIdleCount {
                log.Printf("Завершение по бездействию")
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
        Status: fmt.Sprintf("Completed: %d pages", state.processed),
    }, nil
}
func main() {
    queueConn, err := grpc.Dial("localhost:50052", grpc.WithInsecure())
    if err != nil {
        log.Fatalf("❌ Failed to connect to queue: %v", err)
    }
    defer queueConn.Close()
    
    storageConn, err := grpc.Dial("localhost:50053", grpc.WithInsecure())
    if err != nil {
        log.Fatalf("❌ Failed to connect to storage: %v", err)
    }
    defer storageConn.Close()
    
    config, err := Get_Config()
    if err != nil {
        log.Fatalf("❌ Failed to load config: %v", err)
    }
    
    crawlerServer := NewCrawlerServer(
        queue.NewQueueServiceClient(queueConn),
        storage.NewStorageServiceClient(storageConn),
        config,
    )
    
    if crawlerServer.storageClient == nil {
        log.Fatalf("❌ Storage client is nil!")
    }
    log.Printf("✅ Storage client initialized")
    
    lis, err := net.Listen("tcp", ":50051")
    if err != nil {
        log.Fatalf("❌ Failed to listen: %v", err)
    }

    grpcServer := grpc.NewServer()
    pb.RegisterCrawlerServiceServer(grpcServer, crawlerServer) 
    
    
    stop := make(chan os.Signal, 1)
    signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
    
    go func() {
        <-stop  
        log.Println("🛑 Received shutdown signal...")
        log.Println("⏳ Gracefully stopping gRPC server...")
        grpcServer.GracefulStop()  
        log.Println("✅ gRPC server stopped")
    }()
    
    log.Printf("🚀 gRPC Server starting on port 50051...")
    if err := grpcServer.Serve(lis); err != nil {
        log.Fatalf("❌ Server failed: %v", err)
    }
}

