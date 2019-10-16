package queue

import (
	"context"
	"fmt"
	"os"
	"testing"
)

var config struct {
	conn *Conn
}

func init() {
	conn, err := New(os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	config.conn = conn

	// Ensure the queue starts off empty
	if err := config.conn.Drain(context.Background()); err != nil {
		panic(err)
	}
}

func insertItems(t *testing.T, n int) {
	for i := 0; i < n; i++ {
		err := config.conn.Insert(context.Background(), Item{URL: fmt.Sprintf("https://example.com/%d", i)})
		if err != nil {
			t.Error(err)
		}
	}
}

func TestNext(t *testing.T) {
	n := 10
	insertItems(t, n)

	for i := 0; i < n; i++ {
		item, err := config.conn.Next(context.Background())
		if err != nil {
			t.Error(err)
		}
		item.Done(context.Background())
		item.Close(context.Background())

	}

	_, err := config.conn.Next(context.Background())
	if err != ErrNoItem {
		t.Error("got unexpected item")
	}
}

func TestDrain(t *testing.T) {
	insertItems(t, 1)

	if err := config.conn.Drain(context.Background()); err != nil {
		t.Error(err)
	}

	_, err := config.conn.Next(context.Background())
	if err != ErrNoItem {
		t.Error("got unexpected item")
	}
}
