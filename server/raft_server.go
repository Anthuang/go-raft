package server

import (
	"context"
	"errors"
	"time"

	"github.com/anthuang/go-raft/proto"
)

// RaftServer implements the raft server
type RaftServer struct {
	R *Replica
}

// Get returns a value
func (s RaftServer) Get(ctx context.Context, req *proto.GetReq) (*proto.GetResp, error) {
	s.R.mu.Lock()
	defer s.R.mu.Unlock()

	// Wait for leader election if no known leaders
	for s.R.leader == -1 {
		s.R.mu.Unlock()
		time.Sleep(s.R.pingInterval)
		s.R.mu.Lock()
	}

	done := make(chan bool, len(s.R.peers))

	// Redirect request to real leader if not self
	if s.R.leader != s.R.id {
		var resp *proto.GetResp
		var err error
		go func() {
			resp, err = s.R.extPeers[s.R.leader].Get(ctx, req)
			done <- true
		}()
		for {
			select {
			case <-done:
				return resp, err
			default:
				s.R.mu.Unlock()
				time.Sleep(s.R.pingInterval)
				s.R.mu.Lock()
			}
		}
	}

	return &proto.GetResp{
		Value: s.R.kvStore[req.Key],
	}, nil
}

// Put updates a value
func (s RaftServer) Put(ctx context.Context, req *proto.PutReq) (*proto.PutResp, error) {
	s.R.mu.Lock()
	defer s.R.mu.Unlock()

	// Wait for leader election if no known leaders
	for s.R.leader == -1 {
		s.R.mu.Unlock()
		time.Sleep(s.R.pingInterval)
		s.R.mu.Lock()
	}

	done := make(chan bool, len(s.R.peers))

	// Redirect request to real leader if not self
	if s.R.leader != s.R.id {
		var resp *proto.PutResp
		var err error
		go func() {
			resp, err = s.R.extPeers[s.R.leader].Put(ctx, req)
			done <- true
		}()
		for {
			select {
			case <-done:
				return resp, err
			default:
				s.R.mu.Unlock()
				time.Sleep(s.R.pingInterval)
				s.R.mu.Lock()
			}
		}
	}

	newIndex := int64(len(s.R.log))

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
		var entries []*proto.Entry
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

					req := &proto.AppendEntryReq{
						Entries:    entries,
						Id:         s.R.id,
						LastCommit: s.R.lastCommit,
						PreIndex:   -1,
						PreTerm:    -1,
						Term:       s.R.term,
					}
					if next != 0 {
						req.PreIndex = next - 1
						req.PreTerm = s.R.log[next-1].Term
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
			s.R.lastCommit = newIndex
			s.R.execute()

			return &proto.PutResp{}, nil
		}
	}

	return nil, errors.New("PUT failed")
}
