package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	pb "scratch-chat/proto"

	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedChatServiceServer
}

func (s *server) StreamChat(stream pb.ChatService_StreamChatServer) error {
	log.Println("Connected! to Bidirectional stream")

	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			text := scanner.Text()
			if text == "" {
				continue
			}
			stream.Send(&pb.Response{
				Server: "harsha",
				Text:   text,
			})
			fmt.Print("Server > ")
		}
	}()

	for {
		req, err := stream.Recv()
		if err != nil {
			log.Printf("\nError receiving from client: %v", err)
			return err
		}
		fmt.Printf("\n[%s]: %s\nServer > ", req.Client, req.Text)
		os.Stdout.Sync()
	}

}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterChatServiceServer(grpcServer, &server{})
	log.Println("Server is running on port 50051")
	fmt.Print("Server > ")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
