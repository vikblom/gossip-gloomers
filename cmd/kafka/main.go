package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Msg struct {
	Offset  int
	Content int
}

func (m Msg) MarshalJSON() ([]byte, error) {
	return json.Marshal([]int{m.Offset, m.Content})
}

type Server struct {
	n *maelstrom.Node

	// offsets[k]v has the next integer v for the queue k.
	offsets   map[string]int
	muOffsets sync.Mutex

	logs   map[string][]Msg
	muLogs sync.Mutex

	committed   map[string]int
	muCommitted sync.Mutex
}

func New(n *maelstrom.Node) *Server {

	s := &Server{
		n:         n,
		offsets:   map[string]int{},
		logs:      map[string][]Msg{},
		committed: map[string]int{},
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

func (s *Server) nextOffset(key string) int {
	s.muOffsets.Lock()
	defer s.muOffsets.Unlock()

	v := s.offsets[key]
	s.offsets[key] = v + 1
	return v
}

func (s *Server) store(key string, msg int) int {
	offset := s.nextOffset(key)

	s.muLogs.Lock()
	defer s.muLogs.Unlock()
	log := s.logs[key]
	log = append(log, Msg{Offset: offset, Content: msg})
	s.logs[key] = log

	return offset
}

func (s *Server) retrieve(key string, offset int) []Msg {
	s.muLogs.Lock()
	defer s.muLogs.Unlock()
	log := s.logs[key]

	for i, m := range log {
		if offset <= m.Offset {
			// Re-slice, since we only append, should be fine.
			return log[i:]
		}
	}
	return []Msg{}
}

func (s *Server) commit(key string, offset int) {
	s.muCommitted.Lock()
	defer s.muCommitted.Unlock()
	s.committed[key] = offset // FIXME: Fail if going back?
}

func (s *Server) getCommitted(key string) (int, bool) {
	s.muCommitted.Lock()
	defer s.muCommitted.Unlock()
	v, ok := s.committed[key]
	return v, ok
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

		offset := s.store(body.Key, body.Msg)

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

		out := map[string][]Msg{}
		for key, offset := range body.Offsets {
			out[key] = s.retrieve(key, offset)
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
			s.commit(key, offset)
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
			offset, ok := s.getCommitted(key)
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
	s := New(n)

	if err := s.Run(); err != nil {
		log.Fatal(err)
	}
}
