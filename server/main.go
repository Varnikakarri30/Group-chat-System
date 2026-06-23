package main

import (
	"bufio"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	pb "scratch-chat/proto"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

//go:embed static/*
var staticFS embed.FS

// ChatHub coordinates message routing between gRPC streams and Web HTTP/SSE connections
type ChatHub struct {
	mu                  sync.Mutex
	serverWebChans      map[string][]chan *pb.Request
	grpcServerSendChans map[string]chan *pb.Response
	webClientSendChans  map[string]chan *pb.Request
}

var hub = &ChatHub{
	serverWebChans:      make(map[string][]chan *pb.Request),
	grpcServerSendChans: make(map[string]chan *pb.Response),
	webClientSendChans:  make(map[string]chan *pb.Request),
}

func (h *ChatHub) RegisterServerWeb(serverName string, ch chan *pb.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.serverWebChans[serverName] = append(h.serverWebChans[serverName], ch)
}

func (h *ChatHub) UnregisterServerWeb(serverName string, ch chan *pb.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	chans := h.serverWebChans[serverName]
	for i, c := range chans {
		if c == ch {
			h.serverWebChans[serverName] = append(chans[:i], chans[i+1:]...)
			break
		}
	}
}

func (h *ChatHub) BroadcastToServerWeb(req *pb.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, chans := range h.serverWebChans {
		for _, ch := range chans {
			select {
			case ch <- req:
			default:
			}
		}
	}
}

func (h *ChatHub) RegisterGrpcServerSend(clientName string, ch chan *pb.Response) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.grpcServerSendChans[clientName] = ch
}

func (h *ChatHub) UnregisterGrpcServerSend(clientName string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.grpcServerSendChans, clientName)
}

func (h *ChatHub) SendToGrpcClient(clientName string, res *pb.Response) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if ch, ok := h.grpcServerSendChans[clientName]; ok {
		select {
		case ch <- res:
			return true
		default:
			return false
		}
	}
	return false
}

func (h *ChatHub) RegisterWebClientSend(clientName string, ch chan *pb.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.webClientSendChans[clientName] = ch
}

func (h *ChatHub) UnregisterWebClientSend(clientName string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.webClientSendChans, clientName)
}

func (h *ChatHub) SendToGrpcStream(clientName string, req *pb.Request) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if ch, ok := h.webClientSendChans[clientName]; ok {
		select {
		case ch <- req:
			return true
		default:
			return false
		}
	}
	return false
}

type server struct {
	pb.UnimplementedChatServiceServer
}

// StreamChat is the bidirectional gRPC stream handler on the server
func (s *server) StreamChat(stream pb.ChatService_StreamChatServer) error {
	log.Println("New gRPC client connected to StreamChat")

	var clientName string
	var registered bool
	sendChan := make(chan *pb.Response, 100)

	defer func() {
		if registered && clientName != "" {
			hub.UnregisterGrpcServerSend(clientName)
			log.Printf("gRPC client [%s] stream closed", clientName)
		}
	}()

	// Read responses from web UI/terminal and write to the gRPC client stream
	go func() {
		for res := range sendChan {
			if err := stream.Send(res); err != nil {
				log.Printf("Error writing to gRPC stream: %v", err)
				return
			}
		}
	}()

	// Read requests from gRPC client stream
	for {
		req, err := stream.Recv()
		if err != nil {
			log.Printf("Error reading from gRPC stream: %v", err)
			return err
		}

		// Register client send channel when we see the name for the first time
		if !registered {
			clientName = req.Client
			hub.RegisterGrpcServerSend(clientName, sendChan)
			registered = true
			log.Printf("gRPC client [%s] registered in hub", clientName)
		}

		if req.Text != "" {
			fmt.Printf("\n[%s]: %s\nServer > ", req.Client, req.Text)
			os.Stdout.Sync()
		}

		// Broadcast request to web server panels
		hub.BroadcastToServerWeb(req)
	}
}

// Helper to configure SSE headers
func enableSSE(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
}

// clientEventsHandler handles SSE stream for Client Web Panel
func clientEventsHandler(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, "name query parameter is required", http.StatusBadRequest)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	enableSSE(w)
	flusher.Flush()

	// Dial local gRPC server
	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Client SSE failed to connect to gRPC: %v", err)
		return
	}
	defer conn.Close()

	client := pb.NewChatServiceClient(conn)
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	stream, err := client.StreamChat(ctx)
	if err != nil {
		log.Printf("Client SSE failed to open stream: %v", err)
		return
	}

	sendChan := make(chan *pb.Request, 100)
	hub.RegisterWebClientSend(name, sendChan)
	defer hub.UnregisterWebClientSend(name)

	// Send initial empty message to trigger server registration of this client
	err = stream.Send(&pb.Request{
		Client: name,
		Text:   "",
	})
	if err != nil {
		log.Printf("Client SSE failed to send initial join: %v", err)
		return
	}

	// Goroutine to send messages from Web UI POSTs into gRPC client stream
	go func() {
		for req := range sendChan {
			if err := stream.Send(req); err != nil {
				log.Printf("Error sending message to gRPC: %v", err)
				return
			}
		}
	}()

	// Read responses from gRPC stream and write to HTTP SSE stream
	for {
		res, err := stream.Recv()
		if err != nil {
			log.Printf("Client SSE stream closed: %v", err)
			break
		}

		payload, err := json.Marshal(res)
		if err != nil {
			continue
		}

		_, err = fmt.Fprintf(w, "data: %s\n\n", payload)
		if err != nil {
			break
		}
		flusher.Flush()
	}
}

// clientSendHandler forwards messages sent from Client Web Panel to gRPC
func clientSendHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Client string `json:"client"`
		Text   string `json:"text"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.Client == "" || req.Text == "" {
		http.Error(w, "client and text required", http.StatusBadRequest)
		return
	}

	ok := hub.SendToGrpcStream(req.Client, &pb.Request{
		Client: req.Client,
		Text:   req.Text,
	})

	if !ok {
		http.Error(w, "Client connection not active", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// serverEventsHandler handles SSE stream for Server Web Panel
func serverEventsHandler(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, "name query parameter is required", http.StatusBadRequest)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	enableSSE(w)
	flusher.Flush()

	ch := make(chan *pb.Request, 100)
	hub.RegisterServerWeb(name, ch)
	defer hub.UnregisterServerWeb(name, ch)

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case req := <-ch:
			payload, err := json.Marshal(req)
			if err != nil {
				continue
			}
			_, err = fmt.Fprintf(w, "data: %s\n\n", payload)
			if err != nil {
				return
			}
			flusher.Flush()
		case <-ticker.C:
			_, err := fmt.Fprintf(w, ": ping\n\n")
			if err != nil {
				return
			}
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

// serverSendHandler forwards messages sent from Server Web Panel to client gRPC streams
func serverSendHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Server string `json:"server"`
		Client string `json:"client"`
		Text   string `json:"text"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.Server == "" || req.Client == "" || req.Text == "" {
		http.Error(w, "server, client, and text required", http.StatusBadRequest)
		return
	}

	ok := hub.SendToGrpcClient(req.Client, &pb.Response{
		Server: req.Server,
		Text:   req.Text,
	})

	if !ok {
		http.Error(w, "No active gRPC connection found for target client", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func main() {
	// Start gRPC TCP Listener
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen on 50051: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterChatServiceServer(grpcServer, &server{})
	log.Println("gRPC Server is running on port 50051")

	// Set up HTTP Server
	subFS, err := fs.Sub(staticFS, "static")
	if err != nil {
		log.Fatalf("Failed to sub-embed static folder: %v", err)
	}
	http.Handle("/", http.FileServer(http.FS(subFS)))
	http.HandleFunc("/api/client/events", clientEventsHandler)
	http.HandleFunc("/api/client/send", clientSendHandler)
	http.HandleFunc("/api/server/events", serverEventsHandler)
	http.HandleFunc("/api/server/send", serverSendHandler)

	go func() {
		log.Println("Web Server starting on http://localhost:8080")
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Fatalf("Web Server failed to serve: %v", err)
		}
	}()

	// Unified Terminal input reader (broadcasts OS Stdin input to all active client streams)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		fmt.Print("Server > ")
		for scanner.Scan() {
			text := scanner.Text()
			if text == "" {
				continue
			}
			res := &pb.Response{
				Server: "harsha",
				Text:   text,
			}
			
			hub.mu.Lock()
			sentCount := 0
			for _, ch := range hub.grpcServerSendChans {
				select {
				case ch <- res:
					sentCount++
				default:
				}
			}
			hub.mu.Unlock()

			if sentCount > 0 {
				log.Printf("Broadcasted terminal text to %d clients", sentCount)
			} else {
				log.Println("No active clients connected to receive terminal input")
			}
			fmt.Print("Server > ")
		}
	}()

	// Serve gRPC Server (blocking call)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("gRPC Server failed to serve: %v", err)
	}
}
