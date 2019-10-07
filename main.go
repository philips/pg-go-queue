package main

import (
	"context"
	"fmt"
	"os"

	"github.com/philips/pg-go-queue/queue"
)

func main() {
	conn, err := queue.New(os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connection to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	item, err := conn.Next(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}
	defer item.Close(context.Background())

	fmt.Printf("%v\n", item)
	item.Done(context.Background())
}
