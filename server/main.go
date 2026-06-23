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
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

//go:embed static/*
var staticFS embed.FS

// RoomMember represents a user connected to a specific room
type RoomMember struct {
	UserName string
	SendChan chan *pb.Response
}

// ChatHub coordinates message routing by grouping active client streams into rooms
type ChatHub struct {
	mu                 sync.Mutex
	rooms              map[string][]*RoomMember     // Key: Passkey, Value: active members in that room
	webClientSendChans map[string]chan *pb.Request // Key: UserName + ":" + Passkey, Value: channel for POST -> gRPC relay
}

var hub = &ChatHub{
	rooms:              make(map[string][]*RoomMember),
	webClientSendChans: make(map[string]chan *pb.Request),
}

func (h *ChatHub) RegisterMember(passkey string, userName string, sendChan chan *pb.Response) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.rooms[passkey] = append(h.rooms[passkey], &RoomMember{
		UserName: userName,
		SendChan: sendChan,
	})
	log.Printf("[ROOMS] User '%s' registered in room '%s'. Total members: %d", userName, passkey, len(h.rooms[passkey]))
}

func (h *ChatHub) UnregisterMember(passkey string, userName string, sendChan chan *pb.Response) {
	h.mu.Lock()
	defer h.mu.Unlock()
	members := h.rooms[passkey]
	for i, m := range members {
		if m.UserName == userName && m.SendChan == sendChan {
			h.rooms[passkey] = append(members[:i], members[i+1:]...)
			log.Printf("[ROOMS] User '%s' unregistered from room '%s'. Members left: %d", userName, passkey, len(h.rooms[passkey]))
			break
		}
	}
	if len(h.rooms[passkey]) == 0 {
		delete(h.rooms, passkey)
		log.Printf("[ROOMS] Room '%s' is now empty and has been removed", passkey)
	}
}

func (h *ChatHub) BroadcastToRoom(passkey string, res *pb.Response) {
	h.mu.Lock()
	defer h.mu.Unlock()
	members := h.rooms[passkey]
	log.Printf("[BROADCAST] Distributing message from '%s' to %d members in room '%s'", res.Server, len(members), passkey)
	for _, m := range members {
		select {
		case m.SendChan <- res:
		default:
			// Non-blocking write to avoid hanging if one client is slow
		}
	}
}

func (h *ChatHub) RegisterWebClientSend(key string, ch chan *pb.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.webClientSendChans[key] = ch
}

func (h *ChatHub) UnregisterWebClientSend(key string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.webClientSendChans, key)
}

func (h *ChatHub) SendToGrpcStream(key string, req *pb.Request) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if ch, ok := h.webClientSendChans[key]; ok {
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

// parseClientIdentifier decodes username and passkey from the gRPC Client metadata field
func parseClientIdentifier(clientField string) (string, string) {
	parts := strings.SplitN(clientField, ":", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return clientField, "default"
}

// StreamChat handles incoming bidirectional gRPC streams
func (s *server) StreamChat(stream pb.ChatService_StreamChatServer) error {
	log.Println("[gRPC] New stream connection established")

	var userName string
	var passkey string
	var registered bool
	sendChan := make(chan *pb.Response, 100)

	defer func() {
		if registered && userName != "" && passkey != "" {
			hub.UnregisterMember(passkey, userName, sendChan)
		}
	}()

	// Read responses from channel and write to the gRPC client stream
	go func() {
		for res := range sendChan {
			if err := stream.Send(res); err != nil {
				log.Printf("[gRPC] Write stream error for '%s': %v", userName, err)
				return
			}
		}
	}()

	// Read requests from gRPC stream
	for {
		req, err := stream.Recv()
		if err != nil {
			log.Printf("[gRPC] Read stream disconnected: %v", err)
			return err
		}

		if !registered {
			userName, passkey = parseClientIdentifier(req.Client)
			hub.RegisterMember(passkey, userName, sendChan)
			registered = true
		}

		if req.Text != "" {
			fmt.Printf("\n[%s in %s]: %s\nServer > ", userName, passkey, req.Text)
			os.Stdout.Sync()

			// Broadcast this message to all members in the same room
			hub.BroadcastToRoom(passkey, &pb.Response{
				Server: userName,
				Text:   req.Text,
			})
		}
	}
}

// Configure SSE headers
func enableSSE(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
}

// chatEventsHandler handles real-time message streaming (SSE) to the browser
func chatEventsHandler(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	passkey := r.URL.Query().Get("passkey")
	if name == "" || passkey == "" {
		http.Error(w, "name and passkey parameters are required", http.StatusBadRequest)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	enableSSE(w)
	flusher.Flush()

	// Dial gRPC server locally
	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[Web SSE] Connect to gRPC failed: %v", err)
		return
	}
	defer conn.Close()

	client := pb.NewChatServiceClient(conn)
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	stream, err := client.StreamChat(ctx)
	if err != nil {
		log.Printf("[Web SSE] Start StreamChat failed: %v", err)
		return
	}

	key := name + ":" + passkey
	sendChan := make(chan *pb.Request, 100)
	hub.RegisterWebClientSend(key, sendChan)
	defer hub.UnregisterWebClientSend(key)

	// Send initial empty message to trigger server registration of this client
	err = stream.Send(&pb.Request{
		Client: key,
		Text:   "",
	})
	if err != nil {
		log.Printf("[Web SSE] Join message failed: %v", err)
		return
	}

	// Read from POST send requests and push to gRPC
	go func() {
		for req := range sendChan {
			if err := stream.Send(req); err != nil {
				log.Printf("[Web SSE] Send request failed: %v", err)
				return
			}
		}
	}()

	// Read from gRPC and stream to web SSE
	for {
		res, err := stream.Recv()
		if err != nil {
			log.Printf("[Web SSE] Stream closed: %v", err)
			break
		}

		if res.Text == "" {
			continue // Filter out empty join triggers
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

// chatSendHandler receives chat text from the frontend POST request and forwards it to gRPC
func chatSendHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Name    string `json:"name"`
		Passkey string `json:"passkey"`
		Text    string `json:"text"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.Name == "" || req.Passkey == "" || req.Text == "" {
		http.Error(w, "name, passkey, and text are required", http.StatusBadRequest)
		return
	}

	key := req.Name + ":" + req.Passkey
	ok := hub.SendToGrpcStream(key, &pb.Request{
		Client: key,
		Text:   req.Text,
	})

	if !ok {
		http.Error(w, "No active connection found for this user in the room", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func main() {
	// 1. Start gRPC Listener
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen on gRPC port 50051: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterChatServiceServer(grpcServer, &server{})
	log.Println("gRPC Server is running on port 50051")

	// 2. Set up HTTP Web Server
	subFS, err := fs.Sub(staticFS, "static")
	if err != nil {
		log.Fatalf("Failed to sub-embed static folder: %v", err)
	}
	http.Handle("/", http.FileServer(http.FS(subFS)))
	http.HandleFunc("/api/chat/events", chatEventsHandler)
	http.HandleFunc("/api/chat/send", chatSendHandler)

	go func() {
		log.Println("Web Server starting on http://localhost:8080")
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Fatalf("Web Server failed to serve: %v", err)
		}
	}()

	// 3. Admin Terminal console broadcasts to all active rooms/users
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		fmt.Print("Server > ")
		for scanner.Scan() {
			text := scanner.Text()
			if text == "" {
				continue
			}
			res := &pb.Response{
				Server: "[Admin Broadcast]",
				Text:   text,
			}
			
			hub.mu.Lock()
			sentCount := 0
			for passkey, members := range hub.rooms {
				for _, m := range members {
					select {
					case m.SendChan <- res:
						sentCount++
					default:
					}
				}
				log.Printf("[CONSOLE] Admin message broadcasted to room '%s'", passkey)
			}
			hub.mu.Unlock()

			if sentCount > 0 {
				log.Printf("[CONSOLE] Broadcasted admin text to %d total client streams", sentCount)
			} else {
				log.Println("[CONSOLE] No active clients in any room to receive message")
			}
			fmt.Print("Server > ")
		}
	}()

	// Block on serving gRPC
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("gRPC Server failed to serve: %v", err)
	}
}
