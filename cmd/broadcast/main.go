package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Caster struct {
	messages map[int]struct{}
	mu       sync.Mutex
}

func NewCaster() *Caster {
	return &Caster{
		messages: map[int]struct{}{},
		mu:       sync.Mutex{},
	}
}

func (c *Caster) Push(msg int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messages[msg] = struct{}{}
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

		c.Push(body.Message)

		reply := maelstrom.MessageBody{
			Type:      "broadcast_ok",
			InReplyTo: body.MsgID,
			MsgID:     body.MsgID,
		}
		return n.Send(msg.Src, reply)
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

func handleTopology(n *maelstrom.Node) func(maelstrom.Message) error {
	return func(msg maelstrom.Message) error {
		var body maelstrom.MessageBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

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
	n.Handle("topology", handleTopology(n))

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
