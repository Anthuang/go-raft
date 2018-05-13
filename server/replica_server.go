package server

import (
	"context"
	"errors"
	"sort"
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
		s.R.setLeader(req.Id)
		s.R.lastCommit = req.LastCommit
		s.R.execute()

		// Check if preceding entry exists first, unless first entry
		if req.PreIndex == -1 || (req.PreIndex < int64(len(s.R.log)) && s.R.log[req.PreIndex].Term == req.PreTerm) {
			// Append entries to log
			entries := req.Entries
			sort.Slice(entries, func(i, j int) bool { return entries[i].Index < entries[j].Index })

			numNeed := entries[len(entries)-1].Index + 1 - int64(len(s.R.log))
			if numNeed > 0 {
				s.R.log = append(s.R.log, make([]*proto.Entry, numNeed)...)
			}
			for _, e := range entries {
				// s.R.logger.Infof("%d: %d %d %d", s.R.id, req.PreIndex, len(s.R.log), e.Index)
				s.R.log[e.Index] = e
			}
			// s.R.logger.Infof("%d: %v %d", s.R.id, s.R.log, len(s.R.log))

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
			s.R.setLeader(req.Id)
			s.R.lastCommit = req.LastCommit
			s.R.execute()

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

	if !s.R.voted[req.Term] && req.Term >= s.R.term && req.LastIndex >= int64(len(s.R.log)-1) {
		// s.R.logger.Infof("%d: Accepting vote request from %d for term %d", s.R.id, req.Id, req.Term)
		s.R.leader = -1
		s.R.term = req.Term
		s.R.voted[req.Term] = true
		return &proto.VoteResp{}, nil
	}

	// s.R.logger.Infof("%d: Rejecting vote request from %d for term %d", s.R.id, req.Id, req.Term)
	return &proto.VoteResp{}, errors.New("Rejecting vote request")
}
