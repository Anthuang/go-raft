package main

import (
	"bufio"
	"flag"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/anthuang/go-raft/proto"
	"github.com/anthuang/go-raft/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var (
	replicaID = flag.Int("id", -1, "id of replica")
)

func main() {
	flag.Parse()
	if *replicaID == -1 {
		flag.Usage()
		os.Exit(2)
	}

	logger, _ := zap.NewProduction()
	defer logger.Sync()
	sugar := logger.Sugar()

	addrs := []string{}
	f, err := os.Open("config.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	scan := bufio.NewScanner(f)
	for scan.Scan() {
		fields := strings.Fields(scan.Text())
		addrs = append(addrs, fields[0])
	}

	split := strings.Split(addrs[*replicaID], ":")
	if len(split) != 2 {
		sugar.Fatal("Malformed address")
	}
	port := split[1]

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		sugar.Fatal(err)
	}

	var dialOpts []grpc.DialOption
	dialOpts = append(dialOpts, grpc.WithInsecure())
	dialOpts = append(dialOpts, grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 3 * time.Second}))

	var clients []proto.RaftClient
	for _, addr := range addrs {
		cc, err := grpc.Dial(addr, dialOpts...)
		if err != nil {
			sugar.Fatal("unable to connect to host: %v", err)
		}
		c := proto.NewRaftClient(cc)
		clients = append(clients, c)
	}

	r := server.NewReplica(*replicaID, clients, sugar)
	rs := server.RaftServer{R: r}

	s := grpc.NewServer()
	proto.RegisterRaftServer(s, rs)

	s.Serve(listener)
}
