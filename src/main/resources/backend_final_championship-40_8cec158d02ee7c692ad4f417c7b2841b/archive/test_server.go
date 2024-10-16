package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
)

type Config struct {
	Port         string
	CachesNumber int
	Data         map[string]string
}

type Server struct {
	port   string
	db     *Storage
	caches map[int]*Storage
}

type Storage struct {
	items map[string]string
}

func NewCache() *Storage {
	return &Storage{
		items: make(map[string]string),
	}
}

func NewDB(items map[string]string) *Storage {
	st := &Storage{
		items: make(map[string]string),
	}
	for key, item := range items {
		st.addItem(key, item)
	}
	return st
}

func (storage *Storage) addItem(key string, item string) {
	storage.items[key] = item
}

func (storage *Storage) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		if key := r.URL.Path; key != "" {
			if item, ok := storage.items[key]; ok {
				w.Write([]byte(item))
			} else {
				http.Error(w, "Item is not found", http.StatusNotFound)
			}
		} else {
			http.Error(w, "Invalid request", http.StatusBadRequest)
		}
	case "PUT":
		if key := r.URL.Path; key != "" {
			if bodyBytes, err := io.ReadAll(r.Body); err == nil {
				storage.addItem(key, string(bodyBytes))
				w.Write(make([]byte, 0))
			} else {
				http.Error(w, err.Error(), http.StatusBadRequest)
			}
		} else {
			http.Error(w, "Invalid request", http.StatusBadRequest)
		}
	default:
		http.Error(w, "Unsupported method", http.StatusForbidden)
	}
}

func NewServer(config *Config) *Server {
	server := &Server{
		db:     NewDB(config.Data),
		caches: make(map[int]*Storage),
		port:   config.Port,
	}
	for i := 1; i <= config.CachesNumber; i++ {
		server.caches[i] = NewCache()
	}
	return server
}

func mainHandler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Error", http.StatusNotFound)
}

func NewHttpServer(ctx context.Context, cancel context.CancelFunc, server *Server) *http.Server {
	mux := http.NewServeMux()
	for i, cache := range server.caches {
		mux.Handle("/cache/"+strconv.Itoa(i)+"/", http.StripPrefix("/cache/"+strconv.Itoa(i)+"/", cache))
	}
	mux.Handle("/db/", http.StripPrefix("/db/", server.db))
	mux.HandleFunc("/shutdown", func(w http.ResponseWriter, r *http.Request) {
		cancel()
	})
	mux.HandleFunc("/", mainHandler)
	return &http.Server{
		Addr:    ":" + server.port,
		Handler: mux,
	}
}

func main() {
	if len(os.Args) < 2 {
		log.Fatalln("Expected {config}\n")
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		<-c
		cancel()
	}()

	plan, _ := os.ReadFile(os.Args[1]) // filename is the JSON file to read
	var config Config
	if err := json.Unmarshal(plan, &config); err != nil {
		log.Fatalln(err.Error())
	}
	if config.CachesNumber == 0 {
		log.Fatalln("Caches number is zero\n")
	}

	server := NewServer(&config)
	srv := NewHttpServer(ctx, cancel, server)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatalln(err.Error())
		}
		wg.Done()
	}()
	go func() {
		<-ctx.Done()
		srv.Shutdown(context.Background())
	}()
	wg.Wait()
}
