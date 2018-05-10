package server

import (
	"context"
	"log"
	"net"
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
	assert.Equal(t, replicas[0].leader, replicas[1].leader, "Leader should be 0")
	assert.Equal(t, replicas[0].leader, replicas[2].leader, "Leader should be 0")

	kill(servers, replicas, 0)
	time.Sleep(1 * time.Second)

	assert.Equal(t, int64(1), replicas[1].leader, "Leader should be 1")
	assert.Equal(t, replicas[1].leader, replicas[2].leader, "Leader should be 1")

	restart(servers, replicas, addrs, 0)
	kill(servers, replicas, 1)
	time.Sleep(1 * time.Second)

	assert.Equal(t, int64(0), replicas[0].leader, "Leader should be 0")
	assert.Equal(t, replicas[0].leader, replicas[2].leader, "Leader should be 0")

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
func TestLogSimple(t *testing.T) {
	addrs := []string{":6000", ":6010", ":6020"}
	extAddrs := []string{":6100", ":6110", ":6120"}
	servers, _, replicas, extClients, extServers := startup(addrs, extAddrs)

	extClients[0].Put(context.Background(), &proto.PutReq{Key: t.Name(), Value: "00"})
	time.Sleep(200 * time.Millisecond)
	extClients[1].Put(context.Background(), &proto.PutReq{Key: t.Name(), Value: "01"})
	time.Sleep(200 * time.Millisecond)
	extClients[2].Put(context.Background(), &proto.PutReq{Key: t.Name(), Value: "02"})
	time.Sleep(200 * time.Millisecond)

	for i := range replicas {
		assert.Equal(t, "00", replicas[i].log[0].Value, "Values should match")
		assert.Equal(t, "01", replicas[i].log[1].Value, "Values should match")
		assert.Equal(t, "02", replicas[i].log[2].Value, "Values should match")
	}

	shutdown(append(servers, extServers...), replicas)
}
