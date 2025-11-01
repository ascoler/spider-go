package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
	"encoding/json"
	"net"
	pb "local/crawler/internal/gen/crawler"
	queue "local/crawler/internal/gen/queue"
	storage "local/crawler/internal/gen/storage"
	"github.com/PuerkitoBio/goquery"
	"google.golang.org/grpc"
	
	
	
)
type CrawlerServer struct {
    pb.UnimplementedCrawlerServiceServer
    queueClient   queue.QueueServiceClient
    storageClient storage.StorageServiceClient
    config        Config
}


func NewCrawlerServer(
    queueClient queue.QueueServiceClient,
    storageClient storage.StorageServiceClient,
    config Config,
) *CrawlerServer {
    return &CrawlerServer{
        queueClient:   queueClient,
        storageClient: storageClient,
        config:        config,
    }
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

func checkLink(url string) bool {
    res, err := http.Get(url)
    if err != nil {
        log.Printf("Failed to connect to %s: %v", url, err)
        return false
    }
    defer res.Body.Close() 
    
    return res.StatusCode < 400
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
func (s *CrawlerServer) CreateWorker(jobs <-chan string, result chan<- string, newLinks chan<- string){
    context := context.Background()
    for url := range jobs {
        links,title,content := s.AnalysisLink(context,url, 1) 
        result <- fmt.Sprintf("✅ Worker %s done: %d links, title: %s", url, len(links), title)
        
        
        for _, link := range links {
            newLinks <- link
        }
        if len(content) > 100 {
            log.Printf("Content preview: %s...", content[:100])
        }
    }
}
func (s *CrawlerServer) AnalysisLink(ctx context.Context, url string, depth int) ([]string, string, string) {
	if depth <= 0 {
		return []string{}, "", ""
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
		log.Printf("✅ Page saved to storage: %s", url)
	}
	
	return links, title, content
}

func (s *CrawlerServer) StartCrawling(ctx context.Context, req *pb.StartCrawlingRequest) (*pb.StartCrawlingResponse, error) {
    config, err := Get_Config()
    if err != nil {
        log.Fatal("Error")
    }

    jobs := make(chan string, 1000)
    results := make(chan string, 1000)
    newLinks := make(chan string, 10000)

   
    for w := 1; w <= config.WORKER_POOL_SIZE; w++ {
        go s.CreateWorker(jobs, results, newLinks)
    }

    
    go func() {
        for result := range results {
			
            log.Println(result)
        }
    }()

   
    go func() {
        visited := make(map[string]bool)
        message,err := s.TakeJobs(ctx,jobs,"crawl_queue")
		if err != nil {
        log.Printf("Ошибка TakeJobs: %v", err)
    	} else {
        log.Printf("TakeJobs: %s", message)
    	}
        
        

        

       
        timeout := time.After(30 * time.Second)
        
        for {
            select {
			case <-ctx.Done():
        		return 
            case <-timeout:
                log.Printf("crawling completed by timeout")
                close(jobs)
                close(results)
                close(newLinks)
                return
                
            case link := <-newLinks:
                if !visited[link] && len(visited) < config.MAX_PAGES {
                    visited[link] = true
                    err := s.push_work(ctx,"crawl_queue",[]string{link})
					if err != nil{
						log.Printf("Ошибка push_work: %v ", err)
					}
                    
                    
                }
			default:
				time.Sleep(100 * time.Millisecond)
            }
			
        }
    }()

    return &pb.StartCrawlingResponse{
        
        Status: "started",
    }, nil
}


func main(){
	
	
    queueConn, err := grpc.Dial("localhost:50052", grpc.WithInsecure())
    if err != nil {
        log.Fatalf("❌ Failed to connect to queue: %v", err)
    }
    defer queueConn.Close()
    
    config, err := Get_Config()
    if err != nil {
        log.Fatalf("❌ Failed to load config: %v", err)
    }
    
    
    crawlerServer := NewCrawlerServer(
        queue.NewQueueServiceClient(queueConn),
        nil, 
        config,
    )
    
    lis, err := net.Listen("tcp", ":50051")
    if err != nil {
        log.Fatalf("❌ Failed to listen: %v", err)
    }

    grpcServer := grpc.NewServer()
    pb.RegisterCrawlerServiceServer(grpcServer, crawlerServer) 
    
    log.Printf("🚀 gRPC Server starting on port 50051...")
    if err := grpcServer.Serve(lis); err != nil {
        log.Fatalf("❌ Server failed: %v", err)
    }

}

