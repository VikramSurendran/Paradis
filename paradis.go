package main

import (
	// "context"

	"encoding/json"
	"log"
	"math/rand"
	"os"
	"sync"

	// "github.com/go-redis/redis/v8"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var H = 65536
var SUBMITTER = "n0"
var LEADER = "n1"

type Operation int

// Define enum values
const (
	READ Operation = iota
	WRITE
	CAS
)

type kv_op struct {
	opID   int
	opType int
	opKey  int
	opVal  int
	opVal2 int
}

type proposal struct {
	pri  int
	node int
	op   kv_op
}

type clientRequest struct {
	msg maelstrom.Message
	op  kv_op
}

type submitterState struct {
	committedInd int
	decidedSlots []kv_op
	proposed     map[int]clientRequest
}

type commonState struct {
	committedInd int
	decidedSlots []kv_op
	toBeProposed []kv_op
}
type proposerState struct {
	phase    int
	round    int
	step     int
	currProp proposal
	fbest    proposal
	cbest    proposal
}

type recorderState struct {
	step      int
	firstProp proposal
	prevBest  proposal
	currBest  proposal
	numProps  int
}

func main() {
	n := maelstrom.NewNode()

	/**
	We found it difficult to package the redis setup instructions.
	Thus, we have commented the Redis related operations and included a default kv store
	so that the QuePaxa consensus algorithm can be tested independently.
	**/

	// Redis-related kv store operations
	// client := redis.NewClient(&redis.Options{
	// 	Addr:     "localhost:PORT",
	// 	Password: "",
	// 	DB:       0,
	// })

	// pong, err := client.Ping(context.Background()).Result()
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Fprintf(os.Stderr,"Redis ping response: %v\n", pong)

	// rdsContext := context.Background()
	// client.FlushAll(rdsContext)

	majority := (len(n.NodeIDs()) / 2) + 1
	leaderRound := true

	// Add mutex locks for all of these states
	var s_state = submitterState{committedInd: -1, decidedSlots: []kv_op{}}
	var c_state = commonState{decidedSlots: []kv_op{}, toBeProposed: []kv_op{}, committedInd: -1}
	var p_state = proposerState{step: 4, currProp: nil}
	var r_state = recorderState{}

	kvStore := make(map[int]int)
	kvMtx := sync.RWMutex{}

	broadcastToReplicas := func(msg maelstrom.Message, op kv_op) {
		id := rand.Intn(H)
		op.opID = id
		cliReq := clientRequest{msg: msg, op: op}

		for i := 0; i < len(n.NodeIDs()); i++ {
			if n.ID() == n.NodeIDs()[i] {
				continue
			}
			body := map[string]any{
				"type":      "propose",
				"operation": op,
			}
			n.Send(n.NodeIDs()[i], body)
		}
		s_state.proposed[id] = cliReq
	}

	resetBest := func() {
		p_state.cbest = proposal{pri: -1}
		p_state.fbest = proposal{pri: -1}
		leaderRound = true
	}

	recordOp := func(step int, prop proposal) (int, proposal, proposal) {
		if step == r_state.step {
			if r_state.currBest.pri < prop.pri {
				r_state.currBest = prop
				r_state.numProps += 1
			}
		} else if step > r_state.step {
			if step == r_state.step+1 {
				r_state.prevBest = r_state.currBest
			} else {
				r_state.prevBest = proposal{}
			}
			r_state.step = step
			r_state.firstProp = prop
			r_state.currBest = prop
			r_state.numProps = 1
		}
		return r_state.step, r_state.firstProp, r_state.prevBest
	}

	proposeOp := func(prop proposal) {
		resetBest()
		// p_state.step += 4

		for i := 0; i < len(n.NodeIDs()); i++ {
			if p_state.step%4 == 0 && (p_state.step > 4 || n.ID() != LEADER) {
				prop.pri = rand.Intn(H - 1)
			}
			p_state.currProp = prop

			body := map[string]any{
				"type":     "record",
				"step":     p_state.step,
				"proposal": p_state.currProp,
			}
			n.Send(n.NodeIDs()[i], body)
		}
	}

	applyOp := func(op kv_op) int {
		c_state.decidedSlots = append(c_state.decidedSlots, op)
		c_state.committedInd++

		kvMtx.Lock()
		switch op.opType {
		case int(READ):
			// Redis-related kv store operations
			// value, err := client.Get(context.Background(), op.opKey).Result()
			// if err != nil {
			// 	panic(err)
			// }
			// return value
			value := kvStore[op.opKey]
			return value
		case int(WRITE):
			// Redis-related kv store operations
			// err := client.Set(context.Background(), op.opKey, op.opVal, 0).Err()
			// if err != nil {
			// 	panic(err)
			// }
			kvStore[op.opKey] = op.opVal
		case int(CAS):
			// Redis-related kv store operations
			// value, err := client.Get(context.Background(), op.opKey).Result()
			// if err != nil {
			// 	panic(err)
			// }
			// if value == op.opVal {
			// 	err := client.Set(context.Background(), op.opKey, op.opVal2, 0).Err()
			// 	if err != nil {
			// 		panic(err)
			// 	}
			// }
			if kvStore[op.opKey] == op.opVal {
				kvStore[op.opKey] = op.opVal2
			}
		}
		kvMtx.Unlock()
		return 1
	}

	replyToProxy := func(op kv_op) {
		ret := applyOp(op)
		resp := map[string]any{
			"type":      "op_decision",
			"operation": op,
			"value":     ret,
		}

		n.Send(SUBMITTER, resp)

		if len(c_state.toBeProposed) > 0 {
			nextOp := c_state.toBeProposed[0]
			c_state.toBeProposed = c_state.toBeProposed[1:]
			p_state.step = 4
			prop := proposal{pri: H, node: n.ID(), op: nextOp}
			proposeOp(prop)
		}
	}

	n.Handle("op_decision", func(msg maelstrom.Message) error {

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			// return err
		}

		decided_op, ok := body["operation"].(kv_op)

		if !ok {
			// return errors.New("rec_prop is not of type proposal")
		}

		decided_val := body["value"]

		cliMsg := s_state.proposed[decided_op.opID].msg
		delete(s_state.proposed, decided_op.opID)
		s_state.decidedSlots = append(s_state.decidedSlots, decided_op)
		s_state.committedInd++

		var resp = map[string]any{}
		switch decided_op.opType {
		case int(READ):
			resp = map[string]any{
				"type":  "read_ok",
				"value": decided_val,
			}
		case int(WRITE):
			resp = map[string]any{
				"type": "write_ok",
			}
		case int(CAS):
			resp = map[string]any{
				"type": "cas_ok",
			}
		}

		return n.Reply(cliMsg, resp)
	})

	n.Handle("propose", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		prop_op, ok := body["operation"].(kv_op)

		if !ok {
			// return errors.New("rec_prop is not of type proposal")
		}

		// add locks for c_state
		if len(c_state.toBeProposed) == 0 {
			p_state.step = 4
			prop := proposal{pri: H, node: n.ID(), op: prop_op}
			proposeOp(prop)
		}
		c_state.toBeProposed = append(c_state.toBeProposed, prop_op)

		return nil
	})

	n.Handle("record", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		rec_step := body["step"].(int)
		rec_prop := body["proposal"]

		proposalValue, ok := rec_prop.(proposal)
		if !ok {
			// return errors.New("rec_prop is not of type proposal")
		}

		step, first, best := recordOp(rec_step, proposalValue)

		// Update the message type to return back.
		resp := map[string]any{
			"type":       "record_ok",
			"step":       step,
			"first_prop": first,
			"best_prop":  best,
		}

		// Echo the original message back with the updated message type.
		return n.Reply(msg, resp)
	})

	n.Handle("record_ok", func(msg maelstrom.Message) error {
		var body map[string]interface{}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		rec_step := body["step"].(int)
		rec_first := body["first_prop"]
		rec_best := body["best_prop"]

		firstProp, ok := rec_first.(proposal)
		if !ok {
			// return errors.New("rec_prop is not of type proposal")
		}

		bestProp, ok := rec_best.(proposal)
		if !ok {
			// return errors.New("rec_prop is not of type proposal")
		}

		if rec_step > p_state.step {
			p_state.step = rec_step
			p_state.currProp = firstProp
			resetBest()
		} else if rec_step == p_state.step {
			if rec_step%4 == 0 && leaderRound {
				leaderRound = leaderRound && (firstProp.pri == H)
			}
			if firstProp.pri > p_state.fbest.pri {
				p_state.fbest = firstProp
			}
			if bestProp.pri > p_state.cbest.pri {
				p_state.cbest = bestProp
			}
		}

		if r_state.numProps >= majority && p_state.step == rec_step {
			if p_state.step%4 == 0 {
				if leaderRound {
					replyToProxy(p_state.fbest.op)
					return nil
				} else {
					p_state.currProp = p_state.fbest
				}
			}
			if p_state.step%4 == 1 {
				// Nothing to do
			}
			if p_state.step%4 == 2 {
				if p_state.cbest.op == p_state.currProp.op {
					replyToProxy(p_state.currProp.op)
					return nil
				}
			}
			if p_state.step%4 == 3 {
				p_state.currProp = p_state.cbest
			}
			proposeOp(p_state.currProp)
			p_state.step += 1
			leaderRound = true
		}

		return nil
	})

	/**
		Client to Submitter messages
	**/

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		op_key := int(body["key"].(float64))
		new_op := kv_op{opType: int(READ), opKey: op_key}

		broadcastToReplicas(msg, new_op)

		return nil
	})

	n.Handle("write", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		op_key := int(body["key"].(float64))
		op_val := int(body["value"].(float64))
		new_op := kv_op{opType: int(WRITE), opKey: op_key, opVal: op_val}

		broadcastToReplicas(msg, new_op)

		return nil
	})

	n.Handle("cas", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		op_key := int(body["key"].(float64))
		op_val := int(body["from"].(float64))
		op_val2 := int(body["to"].(float64))
		new_op := kv_op{opType: int(CAS), opKey: op_key, opVal: op_val, opVal2: op_val2}

		broadcastToReplicas(msg, new_op)

		return nil
	})

	if err := n.Run(); err != nil {

		log.Printf("Error: %s", err)
		os.Exit(1)
	}

}
