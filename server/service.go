package server

import (
	"context"
	"errors"
	"time"

	"github.com/anthuang/go-raft/proto"
)

// RaftServer implements the server
type RaftServer struct {
	R *Replica
}

// AppendEntry appends an entry to the replica's log
func (s RaftServer) AppendEntry(ctx context.Context, req *proto.AppendEntryReq) (*proto.AppendEntryResp, error) {
	s.R.lastCommit = req.LastCommit
	s.R.lastPinged = time.Now()
	s.R.setLeader(req.Id)

	// Check if preceding entry exists first
	if req.Index < int64(len(s.R.log)) && s.R.log[req.Index].term == req.Term {
		// Append entries to log
		s.R.log = append(s.R.log, make([]state, len(req.Entries))...)
		for _, e := range req.Entries {
			s.R.log[e.Index] = state{
				command: e.Command,
				index:   e.Index,
			}
		}

		return &proto.AppendEntryResp{Ok: true}, nil
	}
	return &proto.AppendEntryResp{Ok: false}, nil
}

// HeartBeat pings replicas
func (s RaftServer) HeartBeat(ctx context.Context, req *proto.HeartBeatReq) (*proto.HeartBeatResp, error) {
	s.R.lastCommit = req.LastCommit
	s.R.lastPinged = time.Now()
	s.R.setLeader(req.Id)

	// s.R.logger.Infof("Received heartbeat from %d", req.Id)

	return &proto.HeartBeatResp{}, nil
}

// Vote attempts to elect current replica as leader
func (s RaftServer) Vote(ctx context.Context, req *proto.VoteReq) (*proto.VoteResp, error) {
	s.R.lastPinged = time.Now()

	if req.Term >= s.R.term && !s.R.voted[req.Term] {
		// s.R.logger.Infof("Accepting vote request from %d for term %d", req.Id, s.R.term)
		s.R.leader = -1
		s.R.term = req.Term
		s.R.voted[req.Term] = true
		return &proto.VoteResp{}, nil
	}

	// s.R.logger.Infof("Rejecting vote request from %d for term %d", req.Id, s.R.term)
	return &proto.VoteResp{}, errors.New("Rejecting vote request")
}
