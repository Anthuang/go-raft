package server

import (
	"context"
	"errors"
	"time"

	"github.com/anthuang/go-raft/proto"
)

// RaftServer implements the raft server
type RaftServer struct {
	R       *Replica
	Clients []proto.RaftClient
}

// Put updates a value in the replica's kvStore
func (s RaftServer) Put(ctx context.Context, req *proto.PutReq) (*proto.PutResp, error) {
	s.R.mu.Lock()
	defer s.R.mu.Unlock()

	// Wait for leader election if no known leaders
	for s.R.leader == -1 {
		s.R.mu.Unlock()
		time.Sleep(s.R.timeout)
		s.R.mu.Lock()
	}

	// Redirect request to real leader if not self
	if s.R.leader != s.R.id {
		return s.Clients[s.R.leader].Put(ctx, req)
	}

	done := make(chan bool, len(s.R.peers))
	newIndex := int64(len(s.R.log))

	var entries []*proto.Entry
	newEntry := &proto.Entry{
		Command: "PUT",
		Index:   newIndex,
		Key:     req.Key,
		Term:    s.R.term,
		Value:   req.Value,
	}
	// Append new entry to own log
	s.R.log = append(s.R.log, newEntry)

	for i, p := range s.R.peers {
		go func(i int, p proto.ReplicaClient) {
			if i == int(s.R.id) {
				done <- true
			} else {
				for {
					// If no errors, keep trying until log convergence
					next := int64(s.R.nextIndex[i])
					for n := next; n < newIndex; n++ {
						entries = append(entries, s.R.log[n])
					}
					entries = append(entries, newEntry)

					var req *proto.AppendEntryReq
					if next == 0 {
						req = &proto.AppendEntryReq{
							Entries:    entries,
							Id:         s.R.id,
							LastCommit: s.R.lastCommit,
							PreIndex:   -1,
							PreTerm:    -1,
							Term:       s.R.term,
						}
					} else {
						req = &proto.AppendEntryReq{
							Entries:    entries,
							Id:         s.R.id,
							LastCommit: s.R.lastCommit,
							PreIndex:   next - 1,
							PreTerm:    s.R.log[next-1].Term,
							Term:       s.R.term,
						}
					}
					resp, err := p.AppendEntry(context.Background(), req)
					if err == nil {
						if resp.Ok {
							s.R.nextIndex[i]++
							done <- true
							break
						} else {
							s.R.nextIndex[i]--
						}
					} else {
						done <- false
						break
					}
				}
			}
		}(i, p)
	}

	doneNum := 0
	succNum := 0
	for doneNum < len(s.R.peers) {
		// Wait for all to finish unless majority responds
		s.R.mu.Unlock()
		if <-done {
			succNum++
		}
		doneNum++

		s.R.mu.Lock()
		if succNum >= s.R.majority {
			// Entry appended at majority, can apply operation
			s.R.logger.Infof("%d: Entry appended at majority", s.R.id)
			s.R.lastCommit++
			s.R.kvStore[req.Key] = req.Value

			return &proto.PutResp{}, nil
		}
	}

	return nil, errors.New("PUT failed")
}
