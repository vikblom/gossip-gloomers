// Broadcasts gossip periodically, batching updates.
// This lower the msgs/op, at the cost of latency.
package main

import (
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

	// nbr -> msgs
	pending   map[string]map[int]struct{}
	muPending sync.Mutex
}

func NewCaster() *Caster {
	return &Caster{
		messages: map[int]struct{}{},
		mu:       sync.Mutex{},
		pending:  map[string](map[int]struct{}){},
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

func (c *Caster) Gossip(nbr string, msgs []int) {
	c.muPending.Lock()
	defer c.muPending.Unlock()

	tmp, ok := c.pending[nbr]
	if !ok {
		tmp = make(map[int]struct{})
	}
	for _, m := range msgs {
		tmp[m] = struct{}{}
	}
	c.pending[nbr] = tmp // TODO: Is this necessary?
}

func (c *Caster) Pending(nbr string) []int {
	c.muPending.Lock()
	defer c.muPending.Unlock()

	msgs, ok := c.pending[nbr]
	if !ok {
		return nil
	}

	out := []int{}
	for m := range msgs {
		out = append(out, m)
	}
	return out
}

func (c *Caster) Done(nbr string, msgs []int) {
	c.muPending.Lock()
	defer c.muPending.Unlock()

	tmp, ok := c.pending[nbr]
	if !ok {
		return
	}
	for _, m := range msgs {
		delete(tmp, m)
	}
	c.pending[nbr] = tmp // TODO: Is this necessary?
}

type broadcastBody struct {
	maelstrom.MessageBody
	Message int   `json:"message"`
	Extra   []int `json:"extra"`
}

func handleBroadcast(n *maelstrom.Node, c *Caster) func(maelstrom.Message) error {

	return func(msg maelstrom.Message) error {
		var body broadcastBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		msgs := append(body.Extra, body.Message)

		toGossip := []int{}
		for _, m := range msgs {
			firstTime := c.Push(m)
			if firstTime {
				toGossip = append(toGossip, m)
			}
		}

		reply := maelstrom.MessageBody{
			Type:      "broadcast_ok",
			InReplyTo: body.MsgID,
			MsgID:     body.MsgID,
		}
		err := n.Send(msg.Src, reply)
		if err != nil {
			return fmt.Errorf("send: %w", err)
		}

		if len(toGossip) == 0 {
			return nil
		}

		for _, nbr := range c.nbrs { // FIXME: Race
			if nbr == msg.Src {
				continue
			}
			c.Gossip(nbr, msgs)
		}
		return nil
	}
}

func runGossip(n *maelstrom.Node, c *Caster) error {
	for range time.Tick(100 * time.Millisecond) {
		for _, nbr := range c.nbrs {
			nbr := nbr
			msgs := c.Pending(nbr)
			if len(msgs) == 0 {
				continue
			}

			gossip := broadcastBody{
				MessageBody: maelstrom.MessageBody{
					Type: "broadcast",
				},
				Message: msgs[0],
				Extra:   msgs[1:],
			}

			err := n.RPC(nbr, gossip, func(msg maelstrom.Message) error {
				c.Done(nbr, msgs)
				return nil
			})
			if err != nil {
				fmt.Fprintf(os.Stderr, "gossip rpc to %q with %v: %s", nbr, gossip, err)
			}
		}
	}

	return nil
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

		// Use --topology tree4 for a spanning tree.
		// Scales like log(n) instead of sqrt(n).
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

	go runGossip(n, c)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
