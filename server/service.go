package server

import (
	"context"
	"time"

	"github.com/anthuang/go-raft/proto"
)

// RaftServer implements the server
type RaftServer struct {
	R *Replica
}

// HeartBeat pings replicas
func (s RaftServer) HeartBeat(ctx context.Context, req *proto.HeartBeatReq) (*proto.HeartBeatResp, error) {
	s.R.lastPinged = time.Now()

	if s.R.leader != req.Id {
		s.R.leader = req.Id
	}

	return &proto.HeartBeatResp{}, nil
}

// Vote attempts to elect current replica as leader
func (s RaftServer) Vote(ctx context.Context, req *proto.VoteReq) (*proto.VoteResp, error) {
	s.R.lastPinged = time.Now()

	if req.Term >= s.R.term && !s.R.voted {
		s.R.voted = true
		return &proto.VoteResp{Ok: true}, nil
	}

	return &proto.VoteResp{Ok: false}, nil
}
