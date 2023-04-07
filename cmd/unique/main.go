package main

import (
	"encoding/binary"
	"encoding/json"
	"hash/fnv"
	"log"
	"sync/atomic"

	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// hash s into uint64 which is 8 bytes.
func hash(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

// handleGenerate returns a UUID for each request.
// UUID = hash(node id) + monotonic counter
func handleGenerate(n *maelstrom.Node) func(maelstrom.Message) error {
	var counter uint64
	return func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		var bs []byte
		// Two pieces of 8 bytes...
		bs = binary.BigEndian.AppendUint64(bs, hash(n.ID()))
		bs = binary.BigEndian.AppendUint64(bs, counter)
		atomic.AddUint64(&counter, 1)

		// ...fits perfectly in 16 byte UUID.
		id, err := uuid.FromBytes(bs)
		if err != nil {
			return err
		}
		body["id"] = id.String()

		body["type"] = "generate_ok"
		return n.Reply(msg, body)
	}
}

func main() {
	n := maelstrom.NewNode()

	n.Handle("generate", handleGenerate(n))

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
