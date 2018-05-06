package server

import (
	"context"
	"sync"
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
	mu           sync.Mutex
	nextIndex    []int
	peers        []proto.RaftClient
	peersAddrs   []string
	pingInterval time.Duration
	term         int64
	timeout      time.Duration
	voted        map[int64]bool

	initChannel chan bool
}

// NewReplica creates a new Replica object
func NewReplica(id int64, peers []proto.RaftClient, peersAddrs []string, logger *zap.SugaredLogger) *Replica {
	r := &Replica{
		id:           id,
		lastCommit:   -1,
		lastPinged:   time.Now(),
		leader:       -1,
		log:          make([]state, 0),
		logger:       logger,
		majority:     len(peers)/2 + 1,
		mu:           sync.Mutex{},
		nextIndex:    make([]int, len(peers)),
		peers:        peers,
		peersAddrs:   peersAddrs,
		pingInterval: time.Duration(100) * time.Millisecond,
		term:         0,
		timeout:      time.Duration(id*50+1000) * time.Millisecond,
		voted:        make(map[int64]bool),

		initChannel: make(chan bool),
	}

	go r.run()

	return r
}

func (r *Replica) run() {
	// Main replica logic
	r.init()

	for {
		t := time.Now()

		r.mu.Lock()
		if r.leader == r.id && t.Sub(r.lastPinged) > r.pingInterval {
			// Send heart beats
			r.lastPinged = t
			r.heartbeat()
		}

		if r.leader != r.id && t.Sub(r.lastPinged) > r.timeout {
			// Initiate new election
			r.logger.Infof("Initiating election term %d", r.term+1)
			r.term++
			r.leader = -1
			r.mu.Unlock()
			r.vote()
		} else {
			r.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (r *Replica) init() {
	r.logger.Infof("Initializing node %d", r.id)
	done := make(chan bool)
	for i := 0; i < len(r.peers); i++ {
		if i == int(r.id) {
			continue
		}
		go func(i int) {
			for {
				_, err := r.peers[i].HeartBeat(context.Background(), &proto.HeartBeatReq{})
				if err == nil {
					done <- true
					break
				}
			}
		}(i)
	}

	for i := 0; i < len(r.peers)-1; i++ {
		<-done
	}
	r.logger.Infof("Done initializing node %d", r.id)
}

func (r *Replica) vote() {
	// Vote for itself
	done := make(chan bool)
	doneNum := 0
	succNum := 0

	for i, p := range r.peers {
		go func(i int, p proto.RaftClient) {
			_, err := p.Vote(context.Background(), &proto.VoteReq{Id: r.id, Term: r.term})
			if err != nil {
				// r.logger.Infof("%d returned failure for term %d", i, r.term)
				done <- false
			} else {
				// r.logger.Infof("%d returned success for term %d", i, r.term)
				done <- true
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

	r.mu.Lock()
	defer r.mu.Unlock()

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

func (r *Replica) setLeader(id int64) {
	// Set leader and delete voted map
	if r.leader != id {
		r.leader = id
		r.voted = make(map[int64]bool)
	}
}
