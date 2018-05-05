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

// Vote attempts to elect current replica as leader
func (s RaftServer) Vote(ctx context.Context, req *proto.VoteReq) (*proto.VoteResp, error) {
	s.R.lastPinged = time.Now()

	return &proto.VoteResp{
		Ok:   true,
		Term: s.R.term,
	}, nil
}
