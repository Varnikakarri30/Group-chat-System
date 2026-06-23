package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	pb "scratch-chat/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Could not connect to server: %v", err)
	}
	defer conn.Close()

	client := pb.NewChatServiceClient(conn)

	stream, err := client.StreamChat(context.Background())
	if err != nil {
		log.Fatalf("Could not stream chat: %v", err)
	}
	log.Println("connected Successfully")
	fmt.Print("Client > ")

	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			text := scanner.Text()
			if text == "" {
				continue
			}
			err := stream.Send(&pb.Request{
				Client: "Varnikahh",
				Text:   text,
			})
			if err != nil {
				log.Printf("Failed %v", err)
				return
			}
			fmt.Println("Client>")
		}
	}()

	for {
		res, err := stream.Recv()
		if err != nil {
			log.Fatalf("error: %v", err)
		}
		fmt.Printf("\n[%s]: %s\nClient > ", res.Server, res.Text)
		os.Stdout.Sync()
	}
}
