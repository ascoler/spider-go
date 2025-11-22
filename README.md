🕷️ Spider-Go

A smart web crawler that automatically discovers and maps all links on websites.
🚀 What It Does

    Finds all links on web pages

    Processes multiple pages simultaneously for fast performance

    Stores everything in a database

    Automatically distributes tasks between workers

🛠️ How It Works

    Give it a starting URL

    System reads the page and discovers all links

    New links get added to the processing queue

    Repeats for subsequent pages

📦 Components

    Crawler Service (main.go) - Core crawling logic

    Queue Service (queue.go) - Manages URL processing queue

    Storage Service (Work_With_Db.go) - Handles database operations

    API Gateway (Api-Gateway.go) - REST API interface

🏗️ Architecture
text

API Gateway → Crawler Service → Queue Service → Storage Service
     ↓              ↓               ↓               ↓
   :8080          :50051          :50052          :50053

💾 Tech Stack

    Go with gRPC microservices

    MySQL for data storage

    Redis for queue management

    Gin for REST API

🎯 Use Cases

    Discover all links on a website

    Find hidden website sections

    Build complete site maps

    Web content analysis

⚡ Quick Start

    Start all services:
    bash

go run Work_With_Db.go    # Storage service
go run queue.go           # Queue service  
go run main.go            # Crawler service
go run Api-Gateway.go     # API gateway

Send a request:
bash

curl -X POST http://localhost:8080/Analysis_Link \
  -H "Content-Type: application/json" \
  -d '{"url": "https://example.com"}'

The system will crawl the website and return all discovered links and content.
