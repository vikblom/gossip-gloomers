package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Caster struct {
	messages map[int]struct{}
	mu       sync.Mutex

	nbrs   []string
	muNbrs sync.Mutex
}

func NewCaster() *Caster {
	return &Caster{
		messages: map[int]struct{}{},
		mu:       sync.Mutex{},
	}
}

// Push msg to caster, return true if this was new info.
func (c *Caster) Push(msg int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.messages[msg]; ok {
		return false // already present
	}
	c.messages[msg] = struct{}{}
	return true
}

func (c *Caster) Read() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]int, 0, len(c.messages))
	for k := range c.messages {
		out = append(out, k)
	}
	return out
}

func (c *Caster) Nbrs(nbrs []string) {
	c.muNbrs.Lock()
	defer c.muNbrs.Unlock()
	c.nbrs = nbrs
}

func handleBroadcast(n *maelstrom.Node, c *Caster) func(maelstrom.Message) error {

	type broadcastBody struct {
		maelstrom.MessageBody
		Message int `json:"message"`
	}

	return func(msg maelstrom.Message) error {
		var body broadcastBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		firstTime := c.Push(body.Message)

		reply := maelstrom.MessageBody{
			Type:      "broadcast_ok",
			InReplyTo: body.MsgID,
			MsgID:     body.MsgID,
		}
		err := n.Send(msg.Src, reply)
		if err != nil {
			return fmt.Errorf("send: %w", err)
		}

		// gossip on new message?
		if !firstTime {
			return nil
		}
		// Fire and forget, gossip on every change.
		// Will be slow (redundant), and has no retries.
		for _, nbr := range c.nbrs { // FIXME: Race
			gossip := broadcastBody{
				MessageBody: maelstrom.MessageBody{
					Type: "broadcast",
				},
				Message: body.Message,
			}

			go func(nbr string) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				for ctx.Err() == nil {
					err := n.RPC(nbr, gossip, func(msg maelstrom.Message) error {
						cancel()
						return nil
					})
					if err != nil {
						cancel()
						fmt.Fprintf(os.Stderr, "gossip rpc to %q with %v: %s", nbr, gossip, err)
					}
					time.Sleep(100 * time.Millisecond)
				}
			}(nbr)
		}

		return nil
	}
}

func handleRead(n *maelstrom.Node, c *Caster) func(maelstrom.Message) error {

	type readResponse struct {
		maelstrom.MessageBody
		Messages []int `json:"messages"`
	}

	return func(msg maelstrom.Message) error {
		var body maelstrom.MessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		msgs := c.Read()

		reply := readResponse{
			MessageBody: maelstrom.MessageBody{
				Type:      "read_ok",
				InReplyTo: body.MsgID,
				MsgID:     body.MsgID,
			},
			Messages: msgs,
		}
		return n.Send(msg.Src, reply)
	}
}

func handleTopology(n *maelstrom.Node, c *Caster) func(maelstrom.Message) error {

	type topologyBody struct {
		maelstrom.MessageBody
		Topology map[string][]string `json:"topology"`
	}

	return func(msg maelstrom.Message) error {
		var body topologyBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Seems like topology happens before any broadcasts.
		// No need to gossip on topology changes?
		nbrs, ok := body.Topology[n.ID()]
		if ok {
			fmt.Fprintf(os.Stderr, "nbrs are: %v\n", nbrs)
		}
		c.Nbrs(nbrs)

		reply := maelstrom.MessageBody{
			Type:      "topology_ok",
			InReplyTo: body.MsgID,
			MsgID:     body.MsgID,
		}
		return n.Send(msg.Src, reply)
	}
}

func main() {
	c := NewCaster()
	n := maelstrom.NewNode()

	n.Handle("broadcast", handleBroadcast(n, c))
	n.Handle("read", handleRead(n, c))
	n.Handle("topology", handleTopology(n, c))

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
