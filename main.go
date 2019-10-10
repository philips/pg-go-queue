package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/philips/pg-go-queue/queue"
)

func consume(c chan queue.Item, quit chan int) {
	conn, err := queue.New(os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connection to database: %v\n", err)
		os.Exit(1)
	}

	for {
		item, err := conn.Next(context.Background())
		if err == queue.ErrNoItem {
			time.Sleep(time.Second)
			continue
		}
		if err != nil {
			fmt.Printf("consume error: %v\n", err)
			continue
		}
		item.Done(context.Background())
		item.Close(context.Background())
		select {
		case c <- item:
		case <-quit:
			return
		}
	}
}

func main() {
	conn, err := queue.New(os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connection to database: %v\n", err)
		os.Exit(1)
	}

	if err := conn.Drain(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "Drain of queue failed: %v\n", err)
		os.Exit(1)
	}

	count := 100
	for i := 0; i < count; i++ {
		err := conn.Insert(context.Background(), queue.Item{URL: fmt.Sprintf("https://example.com/%d", i)})
		if err != nil {
			panic(err)
		}
	}

	workers := 4
	ch := make(chan queue.Item)
	quit := make(chan int)
	for i := 0; i < workers; i++ {
		go consume(ch, quit)
	}

	for i := 0; i < count; i++ {
		fmt.Printf("%v\n", (<-ch).URL)
	}
	close(quit)
}
