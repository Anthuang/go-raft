package server

import (
	"context"
	"errors"
	"time"

	"github.com/anthuang/go-raft/proto"
	"go.uber.org/zap"
)

// Replica implements main logic for Raft replicas
type Replica struct {
	id         int
	lastPinged time.Time
	leader     bool
	logger     *zap.SugaredLogger
	majority   int
	peers      []proto.RaftClient
	term       int64
	timeout    time.Duration
}

// NewReplica creates a new Replica object
func NewReplica(id int, peers []proto.RaftClient, logger *zap.SugaredLogger) *Replica {
	r := &Replica{
		id:         id,
		lastPinged: time.Now(),
		leader:     false,
		logger:     logger,
		majority:   len(peers)/2 + 1,
		peers:      peers,
		term:       1,
		timeout:    time.Duration(id*50+1000) * time.Millisecond,
	}

	go r.run()

	return r
}

func (r *Replica) run() {
	for {
		t := time.Now()
		if t.Sub(r.lastPinged) > r.timeout {
			// Initiate new election
			r.logger.Infof("Initiating election term %d", r.term)
			r.term++
			err := r.vote()
			if err != nil {
				r.logger.Infof("Election attempt failed")
			}
		}
	}
}

func (r *Replica) vote() error {
	done := make(chan bool)
	doneNum := 0
	succNum := 0
	for i, p := range r.peers {
		go func(i int, p proto.RaftClient) {
			resp, err := p.Vote(context.Background(), &proto.VoteReq{Id: int64(r.id)})
			if err == nil || (resp != nil && resp.Ok) {
				done <- true
			} else {
				done <- false
			}
		}(i, p)
	}

	for doneNum < len(r.peers) {
		ok := <-done
		if ok {
			succNum++
		}
		doneNum++
	}

	if succNum >= r.majority {
		// Become leader
		r.leader = true
		r.logger.Infof("%d is now the leader", r.id)
	} else {
		return errors.New("Not enough OK's from peers")
	}

	return nil
}
