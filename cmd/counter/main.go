package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// The maelstrom helper pkg has some strange behaviour around KV values.
// Use this to get some structure on writes.
// Reads return map[string]any.
type Tag struct {
	Nth   int `json:"nth"`
	Total int `json:"total"`
}

// increment global counter.
// Use the KV store to save [n, total] where n = 0,1,2,...
// and total is the sum of (known) deltas.
// The sequence counter becomes a total order.
// This is needed when using increment for reads, since in
// sequential consistency, a read can return a previous value, as long as it
// is valid to re-order it there. In other words, pure reads can miss writes from
// other nodes.
//
// NOTE: Storing just the total counter works for small n=3 but breaks if n=10.
func increment(kv *maelstrom.KV, delta int) (int, error) {
	for {
		val, err := kv.Read(context.Background(), "total")
		if merr, ok := err.(*maelstrom.RPCError); ok {
			if merr.Code == maelstrom.KeyDoesNotExist {
				val = map[string]any{"nth": 0.0, "total": 0.0}
				err = nil
			}
		}
		if err != nil {
			return 0, fmt.Errorf("kv read int: %w", err)
		}
		vv, ok := val.(map[string]any)
		if !ok {
			return 0, fmt.Errorf("wrong val type: (%T) %v", val, val)
		}
		before := Tag{
			Nth:   int(vv["nth"].(float64)),
			Total: int(vv["total"].(float64)),
		}
		after := Tag{
			Nth:   before.Nth + 1,
			Total: before.Total + delta,
		}

		err = kv.CompareAndSwap(context.Background(), "total", before, after, true)
		if merr, ok := err.(*maelstrom.RPCError); ok {
			if merr.Code == maelstrom.PreconditionFailed {
				continue // Try again.
			}
		}
		if err != nil {
			return 0, fmt.Errorf("kv cas: %w", err)
		}

		return after.Total, nil
	}
}

func handleAdd(n *maelstrom.Node, kv *maelstrom.KV) func(maelstrom.Message) error {
	type addRequest struct {
		maelstrom.MessageBody
		Delta int `json:"delta"`
	}

	return func(msg maelstrom.Message) error {
		var body addRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		_, err := increment(kv, body.Delta)
		if err != nil {
			return fmt.Errorf("increment: %w", err)
		}

		reply := maelstrom.MessageBody{
			Type:      "add_ok",
			MsgID:     body.MsgID,
			InReplyTo: body.MsgID,
		}
		return n.Send(msg.Src, reply)
	}
}

func handleRead(n *maelstrom.Node, kv *maelstrom.KV) func(maelstrom.Message) error {
	type readResponse struct {
		maelstrom.MessageBody
		Value int `json:"value"`
	}

	return func(msg maelstrom.Message) error {
		var body maelstrom.MessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		total, err := increment(kv, 0)
		if err != nil {
			return fmt.Errorf("increment: %w", err)
		}

		reply := readResponse{
			Value: total,
			MessageBody: maelstrom.MessageBody{
				Type:      "read_ok",
				InReplyTo: body.MsgID,
				MsgID:     body.MsgID,
			},
		}
		return n.Send(msg.Src, reply)
	}
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	n.Handle("add", handleAdd(n, kv))
	n.Handle("read", handleRead(n, kv))

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
