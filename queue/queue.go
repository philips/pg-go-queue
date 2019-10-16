package queue

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v4"
)

var ErrNoItem = errors.New("no item in queue")

type Conn struct {
	db *pgx.Conn
}

type Item struct {
	URL      string
	Priority int
	tx       pgx.Tx
}

// New creates a connection to the Postgres databased based on the url provided.
func New(url string) (*Conn, error) {
	conn, err := pgx.Connect(context.Background(), url)
	if err != nil {
		return nil, fmt.Errorf("Unable to connection to database: %v\n", err)
	}

	return &Conn{db: conn}, nil
}

// Close closes out the Postgres database connection.
func (c Conn) Close(ctx context.Context) {
	c.db.Close(ctx)
}

// Insert adds an item to the queue.
func (c *Conn) Insert(ctx context.Context, i Item) (err error) {
	_, err = c.db.Exec(ctx, "INSERT INTO jobs (url) VALUES ($1)", i.URL)
	return
}

// Drain deletes all items in the queue.
func (c *Conn) Drain(ctx context.Context) (err error) {
	_, err = c.db.Exec(ctx, "DELETE FROM jobs")
	return
}

// Next returns the next ordered item in the queue that isn't currently being
// processed or ErrNoItem if no additional items are available.
func (c *Conn) Next(ctx context.Context) (i Item, err error) {
	var tx pgx.Tx
	tx, err = c.db.Begin(ctx)
	if err != nil {
		return
	}

	row := tx.QueryRow(ctx, "SELECT url, priority FROM jobs ORDER BY priority DESC FOR UPDATE skip locked")

	if err = row.Scan(&i.URL, &i.Priority); err != nil {
		if err == pgx.ErrNoRows {
			err = ErrNoItem
		}

		return
	}

	i.tx = *&tx

	return
}

// Done deletes the item from the queue and signals the work has been
// completed. Close does not have to be called after calling Done.
func (i *Item) Done(ctx context.Context) (err error) {
	_, err = i.tx.Exec(ctx, "DELETE FROM jobs WHERE url = $1", i.URL)
	if err != nil {
		return
	}
	return i.Close(ctx)
}

// Close will release the item back to the queue without marking it as Done.
func (i *Item) Close(ctx context.Context) (err error) {
	if i.tx == nil {
		return nil
	}
	err = i.tx.Commit(ctx)
	i.tx = nil
	return err
}
