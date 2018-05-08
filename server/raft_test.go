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

func startup(addrs []string) ([]*grpc.Server, []*grpc.ClientConn, []*Replica, []*RaftServer) {
	var servers []*grpc.Server
	var connections []*grpc.ClientConn
	var replicas []*Replica
	var raftservers []*RaftServer

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
		raftservers = append(raftservers, &rs)

		s := grpc.NewServer()
		proto.RegisterRaftServer(s, rs)

		servers = append(servers, s)

		r.timeout = time.Duration(r.id*100+500) * time.Millisecond

		go func() {
			s.Serve(listener)
		}()
	}

	time.Sleep(2 * time.Second)
	return servers, connections, replicas, raftservers
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
	time.Sleep(2 * time.Second)
}

func restart(servers []*grpc.Server, replicas []*Replica, id int) {
	replicas[id].restart()
	time.Sleep(2 * time.Second)
}

func block(connections []*grpc.ClientConn, id int) {
	connections[id].Close()
	time.Sleep(2 * time.Second)
}

// Leader election tests
func TestLeaderSimple(t *testing.T) {
	addrs := []string{":6000", ":6010", ":6020"}
	servers, _, replicas, _ := startup(addrs)

	assert.Equal(t, int64(0), replicas[0].leader, "Leader should be 0")
	assert.Equal(t, replicas[0].leader, replicas[1].leader, "Leader should be 0")
	assert.Equal(t, replicas[0].leader, replicas[2].leader, "Leader should be 0")

	kill(servers, replicas, 0)

	assert.Equal(t, int64(1), replicas[1].leader, "Leader should be 1")
	assert.Equal(t, replicas[1].leader, replicas[2].leader, "Leader should be 1")

	shutdown(servers, replicas)
}

func TestLeaderOneLeader(t *testing.T) {
	addrs := []string{":6000", ":6010", ":6020"}
	servers, connections, replicas, _ := startup(addrs)

	block(connections, 1)

	assert.Equal(t, replicas[0].leader, replicas[1].leader, "There should only be one leader")
	assert.Equal(t, replicas[1].leader, replicas[2].leader, "There should only be one leader")

	restart(servers, replicas, 0)

	shutdown(servers, replicas)
}
