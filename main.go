package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v4"
)

func main() {
	conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connection to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	rows, err := conn.Query(context.Background(), "select url, priority from jobs")
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()
	for rows.Next() {
		var url string
		var priority int
		if err := rows.Scan(&url, &priority); err != nil {
			fmt.Fprintf(os.Stderr, "rows.Next() failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("%v\n", url)
	}
	if err := rows.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "rows.Next() failed: %v\n", err)
		os.Exit(1)
	}
}
