/*
Package queue is a simple ordered work queue with a Postgres backend.

Items are added to the queue with Insert. The ordering is preserved by Postgres for FIFO prioritization.

Example code:

	conn.Insert(context.Background(), Item{URL: fmt.Sprintf("https://example.com/%d", i)})

This queue is multi-worker safe by using Postgres's "SKIP LOCKED" feature. This is exposed in this Go package through the queue.Next and item.Close methods.

Example code:

	item, err := conn.Next()
	// Do item work here
	item.Close()
*/
package queue
