package queue

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v4"
)

type Conn struct {
	pgx.Conn
}

type Item struct {
	URL      string
	Priority int
	tx       *pgx.Tx
}

func New() Conn {
	conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connection to database: %v\n", err)
		os.Exit(1)
	}

	return Conn{*conn}
}

func (c Conn) Close() {
	c.Close()
}

func (c *Conn) Next() (i Item, err error) {
	*i.tx, err = c.Begin(context.Background())
	if err != nil {
		return
	}

	var rows pgx.Rows
	rows, err = (*i.tx).Query(context.Background(), "SELECT url, priority FROM jobs ORDER BY priority DESC FOR UPDATE skip locked")
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		if err = rows.Scan(&i.URL, &i.Priority); err != nil {
			return
		}
	}
	if err = rows.Err(); err != nil {
		return
	}

	return
}

func (i *Item) Close() (err error) {
	return (*i.tx).Commit(context.Background())
}
