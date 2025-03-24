package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Msgs map[string][][]any

func (msg *Msgs) add(key string, value any) int {
	offset := 0
	_, exists := (*msg)[key]
	if exists && len((*msg)[key]) > 0 {
		offset = (*msg)[key][len((*msg)[key])-1][0].(int) + 1
	}
	(*msg)[key] = append((*msg)[key], []any{offset, value})
	return offset
}

type CommittedOffsets map[string]int

func main() {
	n := maelstrom.NewNode()
	msgs := Msgs{}
	offsets := CommittedOffsets{}

	var mu sync.Mutex

	n.Handle("send", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		mu.Lock()
		offset := msgs.add(body["key"].(string), body["msg"])
		mu.Unlock()

		body["type"] = "send_ok"
		body["offset"] = offset
		delete(body, "msg")
		delete(body, "key")
		return n.Reply(msg, body)
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		tempMsgs := Msgs{}
		mu.Lock()
		for key, pollRangeRaw := range body["offsets"].(map[string]any) {
			pollRange := int(pollRangeRaw.(float64))
			if msgVals, exists := msgs[key]; exists {
				for _, val := range msgVals {
					if val[0].(int) >= pollRange {
						tempMsgs[key] = append(tempMsgs[key], val)
					}
				}
			}
		}
		mu.Unlock()

		body["type"] = "poll_ok"
		body["msgs"] = tempMsgs
		delete(body, "offsets")
		return n.Reply(msg, body)
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		commitOffsetsAny, ok := body["offsets"].(map[string]any)
		if !ok {
			return nil
		}

		mu.Lock()
		for k, v := range commitOffsetsAny {
			if num, ok := v.(float64); ok {
				offsets[k] = int(num)
			} else {
				mu.Unlock()
				return fmt.Errorf("unexpected type for offset: %T", v)
			}
		}
		mu.Unlock()

		body["type"] = "commit_offsets_ok"
		delete(body, "offsets")
		return n.Reply(msg, body)
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		listOffsets := CommittedOffsets{}
		mu.Lock()
		for _, key := range body["keys"].([]any) {
			v, exists := offsets[key.(string)]
			if exists {
				listOffsets[key.(string)] = v
			}
		}
		mu.Unlock()

		body["offsets"] = listOffsets
		body["type"] = "list_committed_offsets_ok"
		delete(body, "keys")
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
