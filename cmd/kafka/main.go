package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"golang.org/x/exp/slices"
)

type Msg struct {
	Offset  int
	Content int
}

func (m *Msg) MarshalJSON() ([]byte, error) {
	return json.Marshal([]int{m.Offset, m.Content})
}

func (m *Msg) UnmarshalJSON(bs []byte) error {
	var arr []int
	err := json.Unmarshal(bs, &arr)
	if err != nil {
		return err
	}
	if len(arr) != 2 {
		return fmt.Errorf("expected 2 ints but got: %v", arr)
	}
	m.Offset = arr[0]
	m.Content = arr[1]
	return nil
}

type Server struct {
	n *maelstrom.Node

	// Linear KV store.
	// "o-key" offsets  (int)
	// "l-key" logs     ([]Msg)
	// "c-key" comitted (int)
	kv *maelstrom.KV
}

func New(n *maelstrom.Node, kv *maelstrom.KV) *Server {

	s := &Server{
		n:  n,
		kv: kv,
	}

	s.n.Handle("send", s.handleSend())
	s.n.Handle("poll", s.handlePoll())
	s.n.Handle("commit_offsets", s.handleCommit())
	s.n.Handle("list_committed_offsets", s.handleList())

	return s
}

func (s *Server) Run() error {
	return s.n.Run()
}

func (s *Server) currentOffset(key string) (int, error) {
	key = "o-" + key
	for {
		v, err := s.kv.ReadInt(context.Background(), key)
		if merr, ok := err.(*maelstrom.RPCError); ok {
			if merr.Code == maelstrom.KeyDoesNotExist {
				return 0, nil
			}
		}
		if err != nil {
			return 0, fmt.Errorf("kv read int %q: %w", key, err)
		}

		err = s.kv.CompareAndSwap(context.Background(), key, v, v, true)
		if merr, ok := err.(*maelstrom.RPCError); ok {
			if merr.Code == maelstrom.PreconditionFailed {
				continue // Try again.
			}
		}
		if err != nil {
			return 0, fmt.Errorf("kv cas %q: %w", key, err)
		}
		return v, nil
	}
}

func (s *Server) nextOffset(key string) (int, error) {
	key = "o-" + key
	for {
		v, err := s.kv.ReadInt(context.Background(), key)
		if merr, ok := err.(*maelstrom.RPCError); ok {
			if merr.Code == maelstrom.KeyDoesNotExist {
				v = 0
				err = nil
			}
		}
		if err != nil {
			return 0, fmt.Errorf("kv read int %q: %w", key, err)
		}

		err = s.kv.CompareAndSwap(context.Background(), key, v, v+1, true)
		if merr, ok := err.(*maelstrom.RPCError); ok {
			if merr.Code == maelstrom.PreconditionFailed {
				continue // Try again.
			}
		}
		if err != nil {
			return 0, fmt.Errorf("kv cas %q: %w", key, err)
		}
		return v, nil
	}
}

func (s *Server) store(key string, msg int) (int, error) {
	offset, err := s.nextOffset(key)
	if err != nil {
		return 0, fmt.Errorf("next offset: %w", err)
	}

	key = "l-" + key
	for {
		var before []Msg
		err := s.kv.ReadInto(context.Background(), key, &before)
		if merr, ok := err.(*maelstrom.RPCError); ok {
			if merr.Code == maelstrom.KeyDoesNotExist {
				err = nil
			}
		}
		if err != nil {
			return 0, fmt.Errorf("kv read into %q: %w", key, err)
		}

		after := append(before, Msg{Offset: offset, Content: msg})
		// slices.SortFunc(after, func(a, b Msg) bool {
		// 	return a.Offset < b.Offset
		// })

		err = s.kv.CompareAndSwap(context.Background(), key, before, after, true)
		if merr, ok := err.(*maelstrom.RPCError); ok {
			if merr.Code == maelstrom.PreconditionFailed {
				continue // Try again.
			}
		}
		if err != nil {
			return 0, fmt.Errorf("kv cas %q: %w", key, err)
		}
		return offset, nil
	}
}

func (s *Server) retrieve(key string, offset int) ([]Msg, error) {

	// "send" will store logs before returning the offset.
	// This guarantees we will read any log which has been sent by a client
	// before calling "poll".
	// But a "send" could grab some offset n, and then work on storing
	// that to the log, while at the same time, another "send" grabs n+1
	// and stores that successfully.
	// We need to detect this by looking at the current context
	// and make sure we read matching state.
	totalOffsets, err := s.currentOffset(key)
	if err != nil {
		return nil, fmt.Errorf("current offset: %w", err)
	}
	if totalOffsets == 0 {
		return []Msg{}, nil
	}

	key = "l-" + key

	var log []Msg
	for len(log) < totalOffsets {
		err := s.kv.ReadInto(context.Background(), key, &log)
		if merr, ok := err.(*maelstrom.RPCError); ok {
			if merr.Code == maelstrom.KeyDoesNotExist {
				return []Msg{}, nil
			}
		}
		if err != nil {
			return nil, fmt.Errorf("kv read into %q: %w", key, err)
		}
	}

	slices.SortFunc(log, func(a, b Msg) bool {
		return a.Offset < b.Offset
	})

	for i, m := range log {
		if offset <= m.Offset {
			// Re-slice, since we only append, should be fine.
			return log[i:], nil
		}
	}
	return []Msg{}, nil
}

func (s *Server) commit(key string, offset int) error {
	key = "c-" + key
	// FIXME: Do we need to check that it's higher?
	err := s.kv.Write(context.Background(), key, offset)
	if err != nil {
		return fmt.Errorf("kv write %q: %w", key, err)
	}
	return nil
}

func (s *Server) getCommitted(key string) (int, bool, error) {
	key = "c-" + key
	v, err := s.kv.ReadInt(context.Background(), key)
	if merr, ok := err.(*maelstrom.RPCError); ok {
		if merr.Code == maelstrom.KeyDoesNotExist {
			return 0, false, nil
		}
	}
	if err != nil {
		return 0, false, fmt.Errorf("kv read %q: %w", key, err)
	}
	return v, true, nil
}

func (s *Server) handleSend() func(maelstrom.Message) error {
	type sendRequest struct {
		maelstrom.MessageBody
		Key string `json:"key"`
		Msg int    `json:"msg"`
	}

	type sendResponse struct {
		maelstrom.MessageBody
		Offset int `json:"offset"`
	}

	return func(msg maelstrom.Message) error {
		var body sendRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offset, err := s.store(body.Key, body.Msg)
		if err != nil {
			return fmt.Errorf("store %q:%v: %w", body.Key, body.Msg, err)
		}

		reply := sendResponse{
			MessageBody: maelstrom.MessageBody{
				Type:      "send_ok",
				MsgID:     body.MsgID,
				InReplyTo: body.MsgID,
			},
			Offset: offset,
		}
		return s.n.Send(msg.Src, reply)
	}
}

func (s *Server) handlePoll() func(maelstrom.Message) error {
	type pollRequest struct {
		maelstrom.MessageBody
		Offsets map[string]int `json:"offsets"`
	}

	type pollResponse struct {
		maelstrom.MessageBody
		Msgs map[string][]Msg `json:"msgs"`
	}

	return func(msg maelstrom.Message) error {
		var body pollRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		var err error
		out := map[string][]Msg{}
		for key, offset := range body.Offsets {
			out[key], err = s.retrieve(key, offset)
			if err != nil {
				return fmt.Errorf("retrieve: %w", err)
			}
		}

		reply := pollResponse{
			MessageBody: maelstrom.MessageBody{
				Type:      "poll_ok",
				MsgID:     body.MsgID,
				InReplyTo: body.MsgID,
			},
			Msgs: out,
		}
		return s.n.Send(msg.Src, reply)
	}
}

func (s *Server) handleCommit() func(maelstrom.Message) error {
	type commitRequest struct {
		maelstrom.MessageBody
		Offsets map[string]int `json:"offsets"`
	}

	return func(msg maelstrom.Message) error {
		var body commitRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for key, offset := range body.Offsets {
			err := s.commit(key, offset)
			if err != nil {
				return fmt.Errorf("commit: %w", err)
			}
		}

		reply := maelstrom.MessageBody{
			Type:      "commit_offsets_ok",
			MsgID:     body.MsgID,
			InReplyTo: body.MsgID,
		}
		return s.n.Send(msg.Src, reply)
	}
}

func (s *Server) handleList() func(maelstrom.Message) error {
	type listRequest struct {
		maelstrom.MessageBody
		Keys []string `json:"keys"`
	}

	type listResponse struct {
		maelstrom.MessageBody
		Offsets map[string]int `json:"offsets"`
	}

	return func(msg maelstrom.Message) error {
		var body listRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		out := map[string]int{}
		for _, key := range body.Keys {
			offset, ok, err := s.getCommitted(key)
			if err != nil {
				return fmt.Errorf("get committed: %w", err)
			}
			if ok {
				out[key] = offset
			}
		}

		reply := listResponse{
			MessageBody: maelstrom.MessageBody{
				Type:      "list_committed_offsets_ok",
				MsgID:     body.MsgID,
				InReplyTo: body.MsgID,
			},
			Offsets: out,
		}
		return s.n.Send(msg.Src, reply)
	}
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	s := New(n, kv)

	if err := s.Run(); err != nil {
		log.Fatal(err)
	}
}
