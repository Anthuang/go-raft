package server

import (
	"context"
	"sync"
	"time"

	"github.com/anthuang/go-raft/proto"
	"go.uber.org/zap"
)

// Replica implements main logic for Raft replicas
type Replica struct {
	id           int64
	isInit       bool
	kvStore      map[string]string
	lastCommit   int64
	lastPinged   time.Time
	leader       int64
	log          []*proto.Entry
	logger       *zap.SugaredLogger
	majority     int
	mu           sync.Mutex
	nextIndex    []int
	peers        []proto.ReplicaClient
	peersAddrs   []string
	pingInterval time.Duration
	shutdown     bool
	term         int64
	timeout      time.Duration
	voted        map[int64]bool

	initChannel chan bool
}

// NewReplica creates a new Replica object
func NewReplica(id int64, peers []proto.ReplicaClient, peersAddrs []string, logger *zap.SugaredLogger) *Replica {
	r := &Replica{
		id:           id,
		isInit:       true,
		kvStore:      make(map[string]string),
		lastCommit:   -1,
		lastPinged:   time.Now(),
		leader:       -1,
		log:          make([]*proto.Entry, 0),
		logger:       logger,
		majority:     len(peers)/2 + 1,
		mu:           sync.Mutex{},
		nextIndex:    make([]int, len(peers)),
		peers:        peers,
		peersAddrs:   peersAddrs,
		pingInterval: time.Duration(100) * time.Millisecond,
		shutdown:     false,
		term:         0,
		timeout:      time.Duration(id*100+500) * time.Millisecond,
		voted:        make(map[int64]bool),

		initChannel: make(chan bool),
	}

	go r.start()

	return r
}

func (r *Replica) start() {
	// Starts with init
	r.init()
	r.run()
}

func (r *Replica) restart() {
	r.leader = -1
	r.shutdown = false
	go r.run()
}

func (r *Replica) init() {
	r.logger.Infof("Initializing node %d", r.id)
	done := make(chan bool, len(r.peers))
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
	r.isInit = false
	r.logger.Infof("Done initializing node %d", r.id)
}

func (r *Replica) run() {
	// Main replica logic
	for !r.shutdown {
		r.mu.Lock()
		t := time.Now()

		if r.leader == r.id && t.Sub(r.lastPinged) > r.pingInterval {
			// Send heart beats
			r.lastPinged = t
			r.heartbeat()
		} else if r.leader != r.id && t.Sub(r.lastPinged) > r.timeout {
			// Initiate new election
			r.logger.Infof("%d: Initiating election term %d", r.id, r.term+1)
			r.term++
			r.leader = -1
			r.vote()
		}

		r.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

func (r *Replica) vote() {
	// Vote for itself
	done := make(chan bool)

	for i, p := range r.peers {
		go func(i int, p proto.ReplicaClient) {
			if i == int(r.id) {
				if !r.voted[r.term] {
					r.leader = -1
					r.voted[r.term] = true
				}
				done <- true
			} else {
				// r.logger.Infof("%d sending to %d", r.id, i)
				_, err := p.Vote(context.Background(), &proto.VoteReq{Id: r.id, Term: r.term})
				if err != nil {
					// r.logger.Infof("%d: %d returned failure for term %d", r.id, i, r.term)
					done <- false
				} else {
					// r.logger.Infof("%d: %d returned success for term %d", r.id, i, r.term)
					done <- true
				}
			}
		}(i, p)
	}

	doneNum := 0
	succNum := 0
	for doneNum < len(r.peers) {
		// Wait for all to finish unless majority responds
		r.mu.Unlock()
		if <-done {
			succNum++
		}
		doneNum++

		r.mu.Lock()
		if succNum >= r.majority {
			// Become leader
			r.logger.Infof("%d is now the leader", r.id)
			r.leader = r.id

			// Set nextIndex to be last index + 1
			for i := range r.nextIndex {
				r.nextIndex[i] = len(r.log)
			}

			r.heartbeat()
			return
		}
	}
	r.logger.Infof("%d: Election attempt failed", r.id)
}

func (r *Replica) heartbeat() {
	// Send heartbeat to followers
	for i, p := range r.peers {
		if i == int(r.id) {
			continue
		}
		go func(p proto.ReplicaClient) {
			p.HeartBeat(context.Background(), &proto.HeartBeatReq{Id: r.id, LastCommit: r.lastCommit, Term: r.term})
		}(p)
	}
}

func (r *Replica) setLeader(id int64) {
	// Set leader and delete voted map
	if r.leader != id {
		r.leader = id
		r.voted = make(map[int64]bool)
	}
}
