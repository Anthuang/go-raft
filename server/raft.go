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
	execUpTo     int64
	extPeers     []proto.RaftClient
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
func NewReplica(id int64, peers []proto.ReplicaClient, peersAddrs []string, extPeers []proto.RaftClient, logger *zap.SugaredLogger) *Replica {
	r := &Replica{
		execUpTo:     -1,
		extPeers:     extPeers,
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
	r.lastPinged = time.Now()
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
			r.sync()
		} else if r.leader != r.id && t.Sub(r.lastPinged) > r.timeout {
			// Initiate new election
			r.logger.Infof("%d: Initiating election term %d", r.id, r.term+1)
			r.term++
			r.leader = -1
			r.vote()
		}

		r.mu.Unlock()
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
				_, err := p.Vote(context.Background(), &proto.VoteReq{Id: r.id, LastIndex: int64(len(r.log) - 1), Term: r.term})
				if err != nil {
					done <- false
				} else {
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

	r.mu.Unlock()
	time.Sleep(50 * time.Millisecond)
	r.mu.Lock()
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
	// Set leader
	if r.leader != id {
		r.leader = id
	}
}

func (r *Replica) execute() {
	// Execute committed entries
	if r.execUpTo < r.lastCommit {
		for i := r.execUpTo + 1; i <= r.lastCommit; i++ {
			if i >= int64(len(r.log)) {
				// If this replica does not have all the entries yet
				break
			}
			entry := r.log[i]
			switch entry.Command {
			case "PUT":
				r.kvStore[entry.Key] = entry.Value
			}
			r.execUpTo++
		}
	}
}

func (r *Replica) sync() {
	// Sync logs with peers
	for i, p := range r.peers {
		if i == int(r.id) {
			continue
		}
		var entries []*proto.Entry
		go func(i int, p proto.ReplicaClient) {
			for {
				// If no errors, keep trying until log convergence
				next := int64(r.nextIndex[i])
				for n := next; n < int64(len(r.log)); n++ {
					entries = append(entries, r.log[n])
				}

				req := &proto.AppendEntryReq{
					Entries:    entries,
					Id:         r.id,
					LastCommit: r.lastCommit,
					PreIndex:   -1,
					PreTerm:    -1,
					Term:       r.term,
				}
				if next != 0 {
					req.PreIndex = next - 1
					req.PreTerm = r.log[next-1].Term
				}
				resp, err := p.AppendEntry(context.Background(), req)
				if err == nil {
					if resp.Ok {
						break
					} else {
						r.nextIndex[i]--
					}
				} else {
					break
				}
			}
		}(i, p)
	}
}
