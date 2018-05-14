package server

import (
	"context"
	"log"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/anthuang/go-raft/proto"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type config struct {
	addrs       []string
	connections []*grpc.ClientConn
	extAddrs    []string
	extClients  []proto.RaftClient
	extServers  []*grpc.Server
	servers     []*grpc.Server
	replicas    []*Replica
}

func startup(addrs []string, extAddrs []string) *config {
	var servers []*grpc.Server
	var connections []*grpc.ClientConn
	var replicas []*Replica
	var extClients []proto.RaftClient
	var extServers []*grpc.Server

	logger, _ := zap.NewDevelopment()
	// logger := zap.NewNop()
	defer logger.Sync()
	sugar := logger.Sugar()

	var dialOpts []grpc.DialOption
	dialOpts = append(dialOpts, grpc.WithInsecure())
	dialOpts = append(dialOpts, grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 3 * time.Second}))

	var clients []proto.ReplicaClient
	for _, addr := range addrs {
		cc, err := grpc.Dial(addr, dialOpts...)
		if err != nil {
			log.Fatalf("unable to connect to host: %v", err)
		}
		c := proto.NewReplicaClient(cc)
		connections = append(connections, cc)
		clients = append(clients, c)
	}

	if len(extAddrs) != 0 {
		for _, a := range extAddrs {
			cc, err := grpc.Dial(a, dialOpts...)
			if err != nil {
				log.Fatalf("unable to connect to host: %v", err)
			}
			c := proto.NewRaftClient(cc)
			connections = append(connections, cc)
			extClients = append(extClients, c)
		}

		for i, a := range extAddrs {
			listener, err := net.Listen("tcp", a)
			if err != nil {
				log.Fatal(err)
			}

			r := NewReplica(int64(i), clients, addrs, extClients, sugar)
			rs := RaftServer{R: r}

			replicas = append(replicas, r)

			r.timeout = time.Duration(r.id*100+500) * time.Millisecond

			s := grpc.NewServer()
			proto.RegisterRaftServer(s, rs)

			extServers = append(extServers, s)

			go func() {
				s.Serve(listener)
			}()
		}
	} else {
		for i := range addrs {
			r := NewReplica(int64(i), clients, addrs, nil, sugar)
			replicas = append(replicas, r)
		}
	}

	for i, a := range addrs {
		listener, err := net.Listen("tcp", a)
		if err != nil {
			log.Fatal(err)
		}

		rs := ReplicaServer{R: replicas[i]}

		s := grpc.NewServer()
		proto.RegisterReplicaServer(s, rs)

		servers = append(servers, s)

		go func() {
			s.Serve(listener)
		}()
	}

	time.Sleep(2 * time.Second)
	return &config{
		addrs:       addrs,
		extAddrs:    extAddrs,
		connections: connections,
		extClients:  extClients,
		extServers:  extServers,
		servers:     servers,
		replicas:    replicas,
	}
}

func shutdown(servers []*grpc.Server, replicas []*Replica) {
	for _, s := range servers {
		s.Stop()
	}
	for _, r := range replicas {
		r.shutdown = true
	}
}

func kill(c *config, id int) {
	c.servers[id].Stop()
	c.replicas[id].shutdown = true
}

func killExt(c *config, id int) {
	c.servers[id].Stop()
	c.extServers[id].Stop()
	c.replicas[id].shutdown = true
}

func restart(c *config, id int) {
	listener, err := net.Listen("tcp", c.addrs[id])
	if err != nil {
		log.Fatal(err)
	}
	s := grpc.NewServer()
	rs := ReplicaServer{R: c.replicas[id]}
	proto.RegisterReplicaServer(s, rs)
	c.servers[id] = s
	go func() {
		s.Serve(listener)
	}()

	var dialOpts []grpc.DialOption
	dialOpts = append(dialOpts, grpc.WithInsecure())
	dialOpts = append(dialOpts, grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 3 * time.Second}))
	cc, err := grpc.Dial(c.addrs[id], dialOpts...)
	if err != nil {
		log.Fatalf("unable to connect to host: %v", err)
	}
	client := proto.NewReplicaClient(cc)
	for i, r := range c.replicas {
		if i == id {
			continue
		}
		r.peers[id] = client
	}

	c.replicas[id].restart()
}

func restartExt(c *config, id int) {
	listener, err := net.Listen("tcp", c.addrs[id])
	if err != nil {
		log.Fatal(err)
	}
	s := grpc.NewServer()
	rs := ReplicaServer{R: c.replicas[id]}
	proto.RegisterReplicaServer(s, rs)
	c.servers[id] = s
	go func() {
		s.Serve(listener)
	}()

	listenerExt, err := net.Listen("tcp", c.extAddrs[id])
	if err != nil {
		log.Fatal(err)
	}
	se := grpc.NewServer()
	rse := RaftServer{R: c.replicas[id]}
	proto.RegisterRaftServer(se, rse)
	c.extServers[id] = se
	go func() {
		se.Serve(listenerExt)
	}()

	var dialOpts []grpc.DialOption
	dialOpts = append(dialOpts, grpc.WithInsecure())
	dialOpts = append(dialOpts, grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 3 * time.Second}))
	cc, err := grpc.Dial(c.addrs[id], dialOpts...)
	if err != nil {
		log.Fatalf("unable to connect to host: %v", err)
	}
	client := proto.NewReplicaClient(cc)

	ccExt, err := grpc.Dial(c.extAddrs[id], dialOpts...)
	if err != nil {
		log.Fatalf("unable to connect to host: %v", err)
	}
	clientExt := proto.NewRaftClient(ccExt)
	c.extClients[id] = clientExt
	for i, r := range c.replicas {
		if i == id {
			continue
		}
		r.peers[id] = client
		r.extPeers[id] = clientExt
	}

	c.replicas[id].restart()
}

func block(c *config, id int) {
	c.connections[id].Close()
}

// Leader election tests
func TestLeaderSimple(t *testing.T) {
	// Simple functionality
	addrs := []string{":6000", ":6010", ":6020"}
	c := startup(addrs, make([]string, 0))

	assert.Equal(t, int64(0), c.replicas[0].leader, "Leader should be 0")
	assert.Equal(t, int64(0), c.replicas[1].leader, "Leader should be 0")
	assert.Equal(t, int64(0), c.replicas[2].leader, "Leader should be 0")

	kill(c, 0)
	time.Sleep(1 * time.Second)

	assert.Equal(t, int64(1), c.replicas[1].leader, "Leader should be 1")
	assert.Equal(t, int64(1), c.replicas[2].leader, "Leader should be 1")

	restart(c, 0)
	kill(c, 1)
	time.Sleep(1 * time.Second)

	assert.Equal(t, int64(0), c.replicas[0].leader, "Leader should be 0")
	assert.Equal(t, int64(0), c.replicas[2].leader, "Leader should be 0")

	shutdown(c.servers, c.replicas)
}

func TestLeaderOneLeader(t *testing.T) {
	// Only one leader can be active
	addrs := []string{":6000", ":6010", ":6020"}
	c := startup(addrs, make([]string, 0))

	block(c, 1)
	time.Sleep(1 * time.Second)

	assert.Equal(t, c.replicas[0].leader, c.replicas[1].leader, "There should only be one leader")
	assert.Equal(t, c.replicas[1].leader, c.replicas[2].leader, "There should only be one leader")

	shutdown(c.servers, c.replicas)
}

func TestLeaderVoteOnce(t *testing.T) {
	// Replicas vote once
	addrs := []string{":6000", ":6010", ":6020"}
	c := startup(addrs, make([]string, 0))

	c.replicas[0].timeout = 3000 * time.Millisecond
	c.replicas[1].timeout = 500 * time.Millisecond
	c.replicas[2].timeout = 500 * time.Millisecond

	kill(c, 0)
	restart(c, 0)
	time.Sleep(3 * time.Second)

	assert.Equal(t, c.replicas[0].leader, c.replicas[1].leader, "There should only be one leader")
	assert.Equal(t, c.replicas[1].leader, c.replicas[2].leader, "There should only be one leader")

	shutdown(c.servers, c.replicas)
}

// Log Replication tests
func TestLogPutSimple(t *testing.T) {
	// Simple PUT functionality
	addrs := []string{":6000", ":6010", ":6020"}
	extAddrs := []string{":6100", ":6110", ":6120"}
	c := startup(addrs, extAddrs)

	nKeys := 50
	values := make(map[string]string)

	for i := 0; i < nKeys; i++ {
		r := rand.Intn(len(addrs))
		key := t.Name() + strconv.Itoa(i)
		value := strconv.Itoa(i)
		values[key] = value
		c.extClients[r].Put(context.Background(), &proto.PutReq{Key: key, Value: value})
	}
	time.Sleep(2 * time.Second)

	for i := 0; i < nKeys; i++ {
		for r := range c.replicas {
			assert.Equal(t, values[c.replicas[r].log[i].Key], c.replicas[r].log[i].Value, "Values should match")
		}
	}

	shutdown(append(c.servers, c.extServers...), c.replicas)
}

func TestLogGetSimple(t *testing.T) {
	// Simple PUT and GET functionality
	addrs := []string{":6000", ":6010", ":6020"}
	extAddrs := []string{":6100", ":6110", ":6120"}
	c := startup(addrs, extAddrs)

	nKeys := 50
	values := make(map[string]string)

	for i := 0; i < nKeys; i++ {
		r := rand.Intn(len(addrs))
		key := t.Name() + strconv.Itoa(i)
		value := strconv.Itoa(i)
		values[key] = value
		c.extClients[r].Put(context.Background(), &proto.PutReq{Key: key, Value: value})
	}
	time.Sleep(2 * time.Second)

	for i := 0; i < nKeys; i++ {
		for r := range c.replicas {
			resp, err := c.extClients[r].Get(context.Background(), &proto.GetReq{Key: c.replicas[r].log[i].Key})
			if err != nil {
				log.Fatal(err)
			}
			assert.Equal(t, values[c.replicas[r].log[i].Key], resp.Value, "Values should match")
		}
	}

	shutdown(append(c.servers, c.extServers...), c.replicas)
}

func TestLogPutConcurrent(t *testing.T) {
	// Concurrent PUT
	addrs := []string{":6000", ":6010", ":6020"}
	extAddrs := []string{":6100", ":6110", ":6120"}
	c := startup(addrs, extAddrs)

	nKeys := 500
	values := make(map[string]string)
	done := make(chan bool, nKeys)

	for i := 0; i < nKeys; i++ {
		key := t.Name() + strconv.Itoa(i)
		value := strconv.Itoa(i)
		values[key] = value
		go func() {
			r := rand.Intn(len(addrs))
			c.extClients[r].Put(context.Background(), &proto.PutReq{Key: key, Value: value})
			done <- true
		}()
	}

	for i := 0; i < nKeys; i++ {
		<-done
	}
	time.Sleep(2 * time.Second)

	for i := 0; i < nKeys; i++ {
		for r := range c.replicas {
			assert.Equal(t, values[c.replicas[r].log[i].Key], c.replicas[r].log[i].Value, "Values should match")
		}
	}

	shutdown(append(c.servers, c.extServers...), c.replicas)
}

func TestLogGetConcurrent(t *testing.T) {
	// Concurrent PUT and GET
	addrs := []string{":6000", ":6010", ":6020"}
	extAddrs := []string{":6100", ":6110", ":6120"}
	c := startup(addrs, extAddrs)

	nKeys := 500
	values := make(map[string]string)
	done := make(chan bool, nKeys)

	for i := 0; i < nKeys; i++ {
		key := t.Name() + strconv.Itoa(i)
		value := strconv.Itoa(i)
		values[key] = value
		go func() {
			r := rand.Intn(len(addrs))
			c.extClients[r].Put(context.Background(), &proto.PutReq{Key: key, Value: value})
			done <- true
		}()
	}

	for i := 0; i < nKeys; i++ {
		<-done
	}
	time.Sleep(2 * time.Second)

	for i := 0; i < nKeys; i++ {
		r := rand.Intn(len(addrs))
		key := c.replicas[r].log[i].Key
		go func() {
			resp, err := c.extClients[r].Get(context.Background(), &proto.GetReq{Key: key})
			if err != nil {
				log.Fatal(err)
			}
			assert.Equal(t, values[key], resp.Value, "Values should match")
			done <- true
		}()
	}
	for i := 0; i < nKeys; i++ {
		<-done
	}

	shutdown(append(c.servers, c.extServers...), c.replicas)
}

func TestLogFailureSimple(t *testing.T) {
	// Log replication amidst failure
	addrs := []string{":6000", ":6010", ":6020"}
	extAddrs := []string{":6100", ":6110", ":6120"}
	c := startup(addrs, extAddrs)

	nKeys := 50
	values := make(map[string]string)

	for i := 0; i < nKeys/2; i++ {
		r := rand.Intn(len(addrs))
		key := t.Name() + strconv.Itoa(i)
		value := strconv.Itoa(i)
		values[key] = value
		c.extClients[r].Put(context.Background(), &proto.PutReq{Key: key, Value: value})
	}

	killExt(c, 0)
	time.Sleep(1 * time.Second)

	for i := nKeys / 2; i < nKeys; i++ {
		r := rand.Intn(len(addrs)-1) + 1
		key := t.Name() + strconv.Itoa(i)
		value := strconv.Itoa(i)
		values[key] = value
		c.extClients[r].Put(context.Background(), &proto.PutReq{Key: key, Value: value})
	}
	time.Sleep(2 * time.Second)

	for i := 0; i < nKeys; i++ {
		for r := range c.replicas {
			if r == 0 {
				continue
			}
			resp, err := c.extClients[r].Get(context.Background(), &proto.GetReq{Key: c.replicas[r].log[i].Key})
			if err != nil {
				log.Fatal(err)
			}
			assert.Equal(t, values[c.replicas[r].log[i].Key], resp.Value, "Values should match")
		}
	}

	shutdown(append(c.servers, c.extServers...), c.replicas)
}

func TestLogFailureRestart(t *testing.T) {
	// Log replication amidst failure and restart
	addrs := []string{":6000", ":6010", ":6020"}
	extAddrs := []string{":6100", ":6110", ":6120"}
	c := startup(addrs, extAddrs)

	nKeys := 50
	values := make(map[string]string)

	for i := 0; i < nKeys/3; i++ {
		r := rand.Intn(len(addrs))
		key := t.Name() + strconv.Itoa(i)
		value := strconv.Itoa(i)
		values[key] = value
		c.extClients[r].Put(context.Background(), &proto.PutReq{Key: key, Value: value})
	}

	killExt(c, 0)
	time.Sleep(1 * time.Second)

	for i := nKeys / 3; i < 2*nKeys/3; i++ {
		r := rand.Intn(len(addrs)-1) + 1
		key := t.Name() + strconv.Itoa(i)
		value := strconv.Itoa(i)
		values[key] = value
		c.extClients[r].Put(context.Background(), &proto.PutReq{Key: key, Value: value})
	}

	restartExt(c, 0)
	time.Sleep(1 * time.Second)

	for i := 2 * nKeys / 3; i < nKeys; i++ {
		r := rand.Intn(len(addrs))
		key := t.Name() + strconv.Itoa(i)
		value := strconv.Itoa(i)
		values[key] = value
		c.extClients[r].Put(context.Background(), &proto.PutReq{Key: key, Value: value})
	}

	time.Sleep(2 * time.Second)

	for i := 0; i < nKeys; i++ {
		for r := range c.replicas {
			resp, err := c.extClients[r].Get(context.Background(), &proto.GetReq{Key: c.replicas[r].log[i].Key})
			if err != nil {
				log.Fatal(err)
			}
			assert.Equal(t, values[c.replicas[r].log[i].Key], resp.Value, "Values should match")
		}
	}

	shutdown(append(c.servers, c.extServers...), c.replicas)
}

func TestLogFailureConcurrent(t *testing.T) {
	// Concurrent failures and restarts
	addrs := []string{":6000", ":6010", ":6020"}
	extAddrs := []string{":6100", ":6110", ":6120"}
	c := startup(addrs, extAddrs)

	nKeys := 200
	nConc := 5
	values := make(map[string]string)
	done := make(chan bool, nKeys)
	stop := make(chan bool)

	killed := -1
	killMu := sync.RWMutex{}
	go func() {
		time.Sleep(2 * time.Second)
		for {
			select {
			case <-stop:
				break
			default:
				killMu.Lock()
				r := rand.Intn(len(addrs))
				killExt(c, r)
				killed = r
				time.Sleep(2 * time.Second)
				killMu.Unlock()

				time.Sleep(3 * time.Second)

				killMu.Lock()
				restartExt(c, r)
				killed = -1
				killMu.Unlock()
				time.Sleep(7 * time.Second)
			}
		}
	}()

	mu := sync.Mutex{}
	for i := 0; i < nConc; i++ {
		go func(i int) {
			for j := 0; j < nKeys/nConc; j++ {
				killMu.RLock()
				idx := j + i*nKeys/nConc
				key := t.Name() + strconv.Itoa(idx)
				value := strconv.Itoa(idx)
				mu.Lock()
				values[key] = value
				mu.Unlock()

				r := rand.Intn(len(addrs))
				for r == killed {
					r = rand.Intn(len(addrs))
				}
				_, err := c.extClients[r].Put(context.Background(), &proto.PutReq{Key: key, Value: value})
				if err != nil {
					log.Fatal(err)
				}

				done <- true
				killMu.RUnlock()
				time.Sleep(200 * time.Millisecond)
			}
		}(i)
	}
	for i := 0; i < nKeys; i++ {
		<-done
	}
	time.Sleep(3 * time.Second)

	for _, r := range c.replicas {
		assert.Equal(t, nKeys, len(r.log), "Log should contain all the updates")
	}

	for i := 0; i < nConc; i++ {
		go func(i int) {
			for j := 0; j < nKeys/nConc; j++ {
				killMu.RLock()
				idx := j + i*nKeys/nConc

				r := rand.Intn(len(addrs))
				for r == killed {
					r = rand.Intn(len(addrs))
				}
				key := c.replicas[r].log[idx].Key
				resp, err := c.extClients[r].Get(context.Background(), &proto.GetReq{Key: key})
				if err != nil {
					log.Fatal(err)
				}
				assert.Equal(t, values[key], resp.Value, "Values should match")

				done <- true
				killMu.RUnlock()
				time.Sleep(200 * time.Millisecond)
			}
		}(i)
	}
	for i := 0; i < nKeys; i++ {
		<-done
	}
	stop <- true

	shutdown(append(c.servers, c.extServers...), c.replicas)
}

func TestLogPassiveCatchUp(t *testing.T) {
	// Logs are passively caught up by leader
	addrs := []string{":6000", ":6010", ":6020"}
	extAddrs := []string{":6100", ":6110", ":6120"}
	c := startup(addrs, extAddrs)

	nKeys := 10

	killExt(c, 2)
	time.Sleep(1 * time.Second)

	for i := 0; i < nKeys; i++ {
		c.extClients[0].Put(context.Background(), &proto.PutReq{
			Key:   t.Name() + strconv.Itoa(i),
			Value: strconv.Itoa(i),
		})
	}

	restartExt(c, 2)
	time.Sleep(3 * time.Second)

	assert.Equal(t, nKeys, len(c.replicas[2].log), "Log should contain all the updates")

	shutdown(append(c.servers, c.extServers...), c.replicas)
}
