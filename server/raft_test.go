package server

import (
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

func startup(addrs []string) ([]*grpc.Server, []*grpc.ClientConn, []*Replica) {
	var servers []*grpc.Server
	var connections []*grpc.ClientConn
	var replicas []*Replica

	logger, _ := zap.NewDevelopment()
	// logger := zap.NewNop()
	defer logger.Sync()
	sugar := logger.Sugar()

	var dialOpts []grpc.DialOption
	dialOpts = append(dialOpts, grpc.WithInsecure())
	dialOpts = append(dialOpts, grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 3 * time.Second}))

	var clients []proto.RaftClient
	for _, addr := range addrs {
		cc, err := grpc.Dial(addr, dialOpts...)
		if err != nil {
			log.Fatalf("unable to connect to host: %v", err)
		}
		c := proto.NewRaftClient(cc)
		connections = append(connections, cc)
		clients = append(clients, c)
	}

	for i, a := range addrs {
		listener, err := net.Listen("tcp", a)
		if err != nil {
			log.Fatal(err)
		}

		r := NewReplica(int64(i), clients, addrs, sugar)
		rs := RaftServer{R: r}

		replicas = append(replicas, r)

		s := grpc.NewServer()
		proto.RegisterRaftServer(s, rs)

		servers = append(servers, s)

		r.timeout = time.Duration(r.id*100+500) * time.Millisecond

		go func() {
			s.Serve(listener)
		}()
	}

	time.Sleep(2 * time.Second)
	return servers, connections, replicas
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
	rs := RaftServer{R: replicas[id]}
	proto.RegisterRaftServer(s, rs)
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
	servers, _, replicas := startup(addrs)

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
	servers, connections, replicas := startup(addrs)

	block(connections, 1)
	time.Sleep(1 * time.Second)

	assert.Equal(t, replicas[0].leader, replicas[1].leader, "There should only be one leader")
	assert.Equal(t, replicas[1].leader, replicas[2].leader, "There should only be one leader")

	shutdown(servers, replicas)
}

func TestLeaderVoteOnce(t *testing.T) {
	// Replicas vote once
	addrs := []string{":6000", ":6010", ":6020"}
	servers, _, replicas := startup(addrs)

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
