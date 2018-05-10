package server

import (
	"context"
	"errors"
	"time"

	"github.com/anthuang/go-raft/proto"
)

// ReplicaServer implements the replicas
type ReplicaServer struct {
	R *Replica
}

// AppendEntry appends an entry to the replica's log
func (s ReplicaServer) AppendEntry(ctx context.Context, req *proto.AppendEntryReq) (*proto.AppendEntryResp, error) {
	s.R.mu.Lock()
	defer s.R.mu.Unlock()

	if req.Term >= s.R.term {
		s.R.lastPinged = time.Now()
		s.R.lastCommit = req.LastCommit
		s.R.setLeader(req.Id)
		s.R.execute(req.LastCommit)

		// Check if preceding entry exists first
		if req.PreIndex < int64(len(s.R.log)) && s.R.log[req.PreIndex].Term == req.PreTerm {
			// Append entries to log
			s.R.log = append(s.R.log, make([]*proto.Entry, len(req.Entries))...)
			for _, e := range req.Entries {
				s.R.log[e.Index] = e
			}

			return &proto.AppendEntryResp{Ok: true}, nil
		}
	}
	return &proto.AppendEntryResp{Ok: false}, nil
}

// HeartBeat receives pings
func (s ReplicaServer) HeartBeat(ctx context.Context, req *proto.HeartBeatReq) (*proto.HeartBeatResp, error) {
	s.R.mu.Lock()
	defer s.R.mu.Unlock()

	if req.Term >= s.R.term {
		s.R.lastPinged = time.Now()

		if !s.R.isInit {
			s.R.term = req.Term
			s.R.lastCommit = req.LastCommit
			s.R.setLeader(req.Id)
			s.R.execute(req.LastCommit)

			// s.R.logger.Infof("%d: Received heartbeat from %d", s.R.id, req.Id)
		}
	}

	return &proto.HeartBeatResp{}, nil
}

// Vote handles vote requests
func (s ReplicaServer) Vote(ctx context.Context, req *proto.VoteReq) (*proto.VoteResp, error) {
	s.R.mu.Lock()
	defer s.R.mu.Unlock()

	s.R.lastPinged = time.Now()

	if req.Term >= s.R.term && !s.R.voted[req.Term] {
		// s.R.logger.Infof("%d: Accepting vote request from %d for term %d", s.R.id, req.Id, req.Term)
		s.R.leader = -1
		s.R.term = req.Term
		s.R.voted[req.Term] = true
		return &proto.VoteResp{}, nil
	}

	// s.R.logger.Infof("%d: Rejecting vote request from %d for term %d", s.R.id, req.Id, req.Term)
	return &proto.VoteResp{}, errors.New("Rejecting vote request")
}