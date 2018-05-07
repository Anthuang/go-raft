package server

import (
	"log"
	"net"
	"testing"
	"time"

	"github.com/anthuang/go-raft/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func startup(addrs []string) ([]*grpc.Server, []*Replica, []*RaftServer) {
	var servers []*grpc.Server
	var replicas []*Replica
	var raftservers []*RaftServer

	logger, _ := zap.NewProduction()
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

		go func() {
			s.Serve(listener)
		}()
	}

	return servers, replicas, raftservers
}

func shutdown(servers []*grpc.Server) {
	for _, s := range servers {
		s.Stop()
	}
}

// Leader election tests
func TestLeaderSimple(t *testing.T) {
	addrs := []string{":6000", ":6010", ":6020"}
	servers, replicas, _ := startup(addrs)

	time.Sleep(2 * time.Second)

	log.Println(replicas[0].leader)
	log.Println(replicas[1].leader)
	log.Println(replicas[2].leader)

	shutdown(servers)
}