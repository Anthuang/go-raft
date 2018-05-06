package server

import (
	"context"
	"time"

	"github.com/anthuang/go-raft/proto"
	"go.uber.org/zap"
)

type state struct {
	command string
	index   int64
	term    int64
}

// Replica implements main logic for Raft replicas
type Replica struct {
	id           int64
	lastCommit   int64
	lastPinged   time.Time
	leader       int64
	log          []state
	logger       *zap.SugaredLogger
	majority     int
	nextIndex    []int
	peers        []proto.RaftClient
	pingInterval time.Duration
	term         int64
	timeout      time.Duration
	voted        bool
}

// NewReplica creates a new Replica object
func NewReplica(id int64, peers []proto.RaftClient, logger *zap.SugaredLogger) *Replica {
	r := &Replica{
		id:           id,
		lastCommit:   -1,
		lastPinged:   time.Now(),
		leader:       -1,
		log:          make([]state, 0),
		logger:       logger,
		majority:     len(peers)/2 + 1,
		nextIndex:    make([]int, len(peers)),
		peers:        peers,
		pingInterval: time.Duration(50) * time.Millisecond,
		term:         0,
		timeout:      time.Duration(id*50+1000) * time.Millisecond,
		voted:        false,
	}

	go r.run()

	return r
}

func (r *Replica) run() {
	// Main replica logic
	for {
		t := time.Now()
		if t.Sub(r.lastPinged) > r.timeout {
			// Initiate new election
			r.logger.Infof("Initiating election term %d", r.term)
			r.term++
			r.leader = -1
			r.vote()
		}

		if r.leader == r.id && t.Sub(r.lastPinged) > r.pingInterval {
			// Send heart beats
			r.lastPinged = t
			r.heartbeat()
		}
	}
}

func (r *Replica) vote() {
	// Vote for itself
	done := make(chan bool)
	doneNum := 0
	succNum := 0

	r.voted = true
	for i, p := range r.peers {
		go func(i int, p proto.RaftClient) {
			resp, err := p.Vote(context.Background(), &proto.VoteReq{Id: r.id, Term: r.term})
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
		r.logger.Infof("%d is now the leader", r.id)
		r.leader = r.id

		// Set nextIndex to be last index + 1
		for i := range r.nextIndex {
			r.nextIndex[i] = len(r.log)
		}
	} else {
		r.logger.Infof("Election attempt failed")
	}
}

func (r *Replica) heartbeat() {
	// Send heartbeat to followers
	for i, p := range r.peers {
		go func(i int, p proto.RaftClient) {
			p.HeartBeat(context.Background(), &proto.HeartBeatReq{Id: r.id, LastCommit: r.lastCommit})
		}(i, p)
	}
}
