package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	ctx := context.Background()

	var count int64 = 0

	n.Handle("init", func(msg maelstrom.Message) error {
		kv.Write(ctx, "0", 0.0)
		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		value, err := kv.Read(ctx, fmt.Sprint(count))
		if err != nil {
			return err
		}

		var floatValue float64
		switch v := value.(type) {
		case int:
			floatValue = float64(v)
		case float64:
			floatValue = v
		default:
			return fmt.Errorf("unexpected type for value: %T", value)
		}

		body["type"] = "read_ok"
		body["value"] = floatValue
		return n.Reply(msg, body)
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta, ok := body["delta"].(float64)
		if !ok {
			return fmt.Errorf("invalid delta type: %T", body["delta"])
		}

		latest, err := kv.Read(ctx, fmt.Sprint(count))
		if err != nil {
			return err
		}

		var latestFloat float64
		switch v := latest.(type) {
		case int:
			latestFloat = float64(v)
		case float64:
			latestFloat = v
		default:
			return fmt.Errorf("unexpected type for latest value: %T", latest)
		}

		if delta != latestFloat {
			kv.Write(ctx, fmt.Sprint(count), delta)
			count++
			var wg sync.WaitGroup
			defer wg.Wait()
			wg.Add(len(n.NodeIDs()))
		}

		delete(body, "delta")
		body["type"] = "add_ok"
		return n.Reply(msg, body)
	})

	err := n.Run()
	if err != nil {
		log.Fatal(err)
	}
}
