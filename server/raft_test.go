package server

import (
	"context"
	"log"
	"math/rand"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/anthuang/go-raft/proto"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func startup(addrs []string, extAddrs []string) ([]*grpc.Server, []*grpc.ClientConn, []*Replica, []proto.RaftClient, []*grpc.Server) {
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

	for i, a := range addrs {
		listener, err := net.Listen("tcp", a)
		if err != nil {
			log.Fatal(err)
		}

		r := NewReplica(int64(i), clients, addrs, sugar)
		rs := ReplicaServer{R: r}

		replicas = append(replicas, r)

		s := grpc.NewServer()
		proto.RegisterReplicaServer(s, rs)

		servers = append(servers, s)

		r.timeout = time.Duration(r.id*100+500) * time.Millisecond

		go func() {
			s.Serve(listener)
		}()
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

			rs := RaftServer{R: replicas[i], Clients: extClients}

			s := grpc.NewServer()
			proto.RegisterRaftServer(s, rs)

			extServers = append(extServers, s)

			go func() {
				s.Serve(listener)
			}()
		}
	}

	time.Sleep(2 * time.Second)
	return servers, connections, replicas, extClients, extServers
}

func shutdown(servers []*grpc.Server, replicas []*Replica) {
	for _, s := range servers {
		s.Stop()
	}
	for _, r := range replicas {
		r.shutdown = true
	}
}

func kill(servers []*grpc.Server, replicas []*Replica, id int) {
	servers[id].Stop()
	replicas[id].shutdown = true
}

func restart(servers []*grpc.Server, replicas []*Replica, addrs []string, id int) {
	listener, err := net.Listen("tcp", addrs[id])
	if err != nil {
		log.Fatal(err)
	}
	s := grpc.NewServer()
	rs := ReplicaServer{R: replicas[id]}
	proto.RegisterReplicaServer(s, rs)
	servers[id] = s
	go func() {
		s.Serve(listener)
	}()
	replicas[id].restart()

	// var dialOpts []grpc.DialOption
	// dialOpts = append(dialOpts, grpc.WithInsecure())
	// dialOpts = append(dialOpts, grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 3 * time.Second}))

	// cc, err := grpc.Dial(addrs[id], dialOpts...)
	// if err != nil {
	// 	log.Fatalf("unable to connect to host: %v", err)
	// }
	// c := proto.NewReplicaClient(cc)

	// for i, r := range replicas {
	// 	if i == id {
	// 		continue
	// 	}
	// 	r.peers[id] = c
	// }
}

func block(connections []*grpc.ClientConn, id int) {
	connections[id].Close()
}

// Leader election tests
func TestLeaderSimple(t *testing.T) {
	// Simple functionality
	addrs := []string{":6000", ":6010", ":6020"}
	servers, _, replicas, _, _ := startup(addrs, make([]string, 0))

	assert.Equal(t, int64(0), replicas[0].leader, "Leader should be 0")
	assert.Equal(t, int64(0), replicas[1].leader, "Leader should be 0")
	assert.Equal(t, int64(0), replicas[2].leader, "Leader should be 0")

	kill(servers, replicas, 0)
	time.Sleep(1 * time.Second)

	assert.Equal(t, int64(1), replicas[1].leader, "Leader should be 1")
	assert.Equal(t, int64(1), replicas[2].leader, "Leader should be 1")

	restart(servers, replicas, addrs, 0)
	kill(servers, replicas, 1)
	time.Sleep(1 * time.Second)

	assert.Equal(t, int64(0), replicas[0].leader, "Leader should be 0")
	assert.Equal(t, int64(0), replicas[2].leader, "Leader should be 0")

	shutdown(servers, replicas)
}

func TestLeaderOneLeader(t *testing.T) {
	// Only one leader can be active
	addrs := []string{":6000", ":6010", ":6020"}
	servers, connections, replicas, _, _ := startup(addrs, make([]string, 0))

	block(connections, 1)
	time.Sleep(1 * time.Second)

	assert.Equal(t, replicas[0].leader, replicas[1].leader, "There should only be one leader")
	assert.Equal(t, replicas[1].leader, replicas[2].leader, "There should only be one leader")

	shutdown(servers, replicas)
}

func TestLeaderVoteOnce(t *testing.T) {
	// Replicas vote once
	addrs := []string{":6000", ":6010", ":6020"}
	servers, _, replicas, _, _ := startup(addrs, make([]string, 0))

	replicas[0].timeout = 3000 * time.Millisecond
	replicas[1].timeout = 500 * time.Millisecond
	replicas[2].timeout = 500 * time.Millisecond

	kill(servers, replicas, 0)
	restart(servers, replicas, addrs, 0)
	time.Sleep(3 * time.Second)

	assert.Equal(t, replicas[0].leader, replicas[1].leader, "There should only be one leader")
	assert.Equal(t, replicas[1].leader, replicas[2].leader, "There should only be one leader")

	shutdown(servers, replicas)
}

// Log Replication tests
func TestLogPutSimple(t *testing.T) {
	// Simple PUT functionality
	addrs := []string{":6000", ":6010", ":6020"}
	extAddrs := []string{":6100", ":6110", ":6120"}
	servers, _, replicas, extClients, extServers := startup(addrs, extAddrs)

	nKeys := 50
	values := make(map[string]string)

	for i := 0; i < nKeys; i++ {
		r := rand.Intn(len(addrs))
		key := t.Name() + strconv.Itoa(i)
		value := strconv.Itoa(i)
		values[key] = value
		extClients[r].Put(context.Background(), &proto.PutReq{Key: key, Value: value})
	}
	time.Sleep(2 * time.Second)

	for i := 0; i < nKeys; i++ {
		for r := range replicas {
			assert.Equal(t, values[replicas[r].log[i].Key], replicas[r].log[i].Value, "Values should match")
		}
	}

	shutdown(append(servers, extServers...), replicas)
}

func TestLogGetSimple(t *testing.T) {
	// Simple PUT and GET functionality
	addrs := []string{":6000", ":6010", ":6020"}
	extAddrs := []string{":6100", ":6110", ":6120"}
	servers, _, replicas, extClients, extServers := startup(addrs, extAddrs)

	nKeys := 50
	values := make(map[string]string)

	for i := 0; i < nKeys; i++ {
		r := rand.Intn(len(addrs))
		key := t.Name() + strconv.Itoa(i)
		value := strconv.Itoa(i)
		values[key] = value
		extClients[r].Put(context.Background(), &proto.PutReq{Key: key, Value: value})
	}
	time.Sleep(2 * time.Second)

	for i := 0; i < nKeys; i++ {
		for r := range replicas {
			resp, err := extClients[r].Get(context.Background(), &proto.GetReq{Key: replicas[r].log[i].Key})
			if err != nil {
				log.Fatal(err)
			}
			assert.Equal(t, values[replicas[r].log[i].Key], resp.Value, "Values should match")
		}
	}

	shutdown(append(servers, extServers...), replicas)
}

func TestLogPutConcurrent(t *testing.T) {
	// Concurrent PUT
	addrs := []string{":6000", ":6010", ":6020"}
	extAddrs := []string{":6100", ":6110", ":6120"}
	servers, _, replicas, extClients, extServers := startup(addrs, extAddrs)

	nKeys := 500
	values := make(map[string]string)
	done := make(chan bool, nKeys)

	for i := 0; i < nKeys; i++ {
		key := t.Name() + strconv.Itoa(i)
		value := strconv.Itoa(i)
		values[key] = value
		go func() {
			r := rand.Intn(len(addrs))
			extClients[r].Put(context.Background(), &proto.PutReq{Key: key, Value: value})
			done <- true
		}()
	}

	for i := 0; i < nKeys; i++ {
		<-done
	}
	time.Sleep(2 * time.Second)

	for i := 0; i < nKeys; i++ {
		for r := range replicas {
			assert.Equal(t, values[replicas[r].log[i].Key], replicas[r].log[i].Value, "Values should match")
		}
	}

	shutdown(append(servers, extServers...), replicas)
}

func TestLogGetConcurrent(t *testing.T) {
	// Concurrent PUT and GET
	addrs := []string{":6000", ":6010", ":6020"}
	extAddrs := []string{":6100", ":6110", ":6120"}
	servers, _, replicas, extClients, extServers := startup(addrs, extAddrs)

	nKeys := 500
	values := make(map[string]string)
	done := make(chan bool, nKeys)

	for i := 0; i < nKeys; i++ {
		key := t.Name() + strconv.Itoa(i)
		value := strconv.Itoa(i)
		values[key] = value
		go func() {
			r := rand.Intn(len(addrs))
			extClients[r].Put(context.Background(), &proto.PutReq{Key: key, Value: value})
			done <- true
		}()
	}

	for i := 0; i < nKeys; i++ {
		<-done
	}
	time.Sleep(2 * time.Second)

	for i := 0; i < nKeys; i++ {
		r := rand.Intn(len(addrs))
		key := replicas[r].log[i].Key
		go func() {
			resp, err := extClients[r].Get(context.Background(), &proto.GetReq{Key: key})
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

	shutdown(append(servers, extServers...), replicas)
}

func TestLogFailureSimple(t *testing.T) {
	// Log replication amidst failure
	addrs := []string{":6000", ":6010", ":6020"}
	extAddrs := []string{":6100", ":6110", ":6120"}
	servers, _, replicas, extClients, extServers := startup(addrs, extAddrs)

	nKeys := 50
	values := make(map[string]string)

	for i := 0; i < nKeys/2; i++ {
		r := rand.Intn(len(addrs))
		key := t.Name() + strconv.Itoa(i)
		value := strconv.Itoa(i)
		values[key] = value
		extClients[r].Put(context.Background(), &proto.PutReq{Key: key, Value: value})
	}

	// Kill 0
	kill(servers, replicas, 0)
	time.Sleep(1 * time.Second)

	for i := nKeys / 2; i < nKeys; i++ {
		r := rand.Intn(len(addrs)-1) + 1
		key := t.Name() + strconv.Itoa(i)
		value := strconv.Itoa(i)
		values[key] = value
		extClients[r].Put(context.Background(), &proto.PutReq{Key: key, Value: value})
	}
	time.Sleep(2 * time.Second)

	for i := 0; i < nKeys; i++ {
		for r := range replicas {
			if r == 0 {
				continue
			}
			resp, err := extClients[r].Get(context.Background(), &proto.GetReq{Key: replicas[r].log[i].Key})
			if err != nil {
				log.Fatal(err)
			}
			assert.Equal(t, values[replicas[r].log[i].Key], resp.Value, "Values should match")
		}
	}

	shutdown(append(servers, extServers...), replicas)
}

func TestLogFailureRestart(t *testing.T) {
	// Log replication amidst failure and restart
	addrs := []string{":6000", ":6010", ":6020"}
	extAddrs := []string{":6100", ":6110", ":6120"}
	servers, _, replicas, extClients, extServers := startup(addrs, extAddrs)

	nKeys := 50
	values := make(map[string]string)

	for i := 0; i < nKeys/3; i++ {
		r := rand.Intn(len(addrs))
		key := t.Name() + strconv.Itoa(i)
		value := strconv.Itoa(i)
		values[key] = value
		extClients[r].Put(context.Background(), &proto.PutReq{Key: key, Value: value})
	}

	// Kill 0
	kill(servers, replicas, 0)
	time.Sleep(1 * time.Second)

	for i := nKeys / 3; i < 2*nKeys/3; i++ {
		r := rand.Intn(len(addrs)-1) + 1
		key := t.Name() + strconv.Itoa(i)
		value := strconv.Itoa(i)
		values[key] = value
		extClients[r].Put(context.Background(), &proto.PutReq{Key: key, Value: value})
	}

	// Restart 0
	restart(servers, replicas, addrs, 0)
	time.Sleep(1 * time.Second)

	for i := 2 * nKeys / 3; i < nKeys; i++ {
		r := rand.Intn(len(addrs))
		key := t.Name() + strconv.Itoa(i)
		value := strconv.Itoa(i)
		values[key] = value
		extClients[r].Put(context.Background(), &proto.PutReq{Key: key, Value: value})
	}

	time.Sleep(2 * time.Second)

	for i := 0; i < nKeys; i++ {
		for r := range replicas {
			resp, err := extClients[r].Get(context.Background(), &proto.GetReq{Key: replicas[r].log[i].Key})
			if err != nil {
				log.Fatal(err)
			}
			assert.Equal(t, values[replicas[r].log[i].Key], resp.Value, "Values should match")
		}
	}

	shutdown(append(servers, extServers...), replicas)
}
