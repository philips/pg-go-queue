package queue

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4"
)

type Conn struct {
	db *pgx.Conn
}

type Item struct {
	URL      string
	Priority int
	tx       pgx.Tx
}

func New(url string) (*Conn, error) {
	conn, err := pgx.Connect(context.Background(), url)
	if err != nil {
		return nil, fmt.Errorf("Unable to connection to database: %v\n", err)
	}

	return &Conn{db: conn}, nil
}

func (c Conn) Close(ctx context.Context) {
	c.db.Close(ctx)
}

func (c *Conn) Next(ctx context.Context) (i Item, err error) {
	var tx pgx.Tx
	tx, err = c.db.Begin(ctx)
	if err != nil {
		return
	}

	row := tx.QueryRow(ctx, "SELECT url, priority FROM jobs ORDER BY priority DESC FOR UPDATE skip locked")

	if err = row.Scan(&i.URL, &i.Priority); err != nil {
		return
	}

	i.tx = *&tx

	return
}

func (i *Item) Close(ctx context.Context) (err error) {
	return i.tx.Commit(ctx)
}

func (i *Item) Done(ctx context.Context) (err error) {
	_, err = i.tx.Exec(ctx, "DELETE FROM jobs WHERE url = $1", i.URL)
	return
}
