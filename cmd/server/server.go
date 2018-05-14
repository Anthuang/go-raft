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
	configPath = flag.String("config", "", "path to config")
	replicaID  = flag.Int("id", -1, "id of replica")
)

func main() {
	flag.Parse()
	if *configPath == "" || *replicaID == -1 {
		flag.Usage()
		os.Exit(2)
	}

	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugar := logger.Sugar()

	addrs := []string{}
	extAddrs := []string{}
	f, err := os.Open(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	scan := bufio.NewScanner(f)
	for scan.Scan() {
		fields := strings.Fields(scan.Text())
		addrs = append(addrs, fields[0])
		extAddrs = append(extAddrs, fields[1])
	}

	split := strings.Split(addrs[*replicaID], ":")
	if len(split) != 2 {
		sugar.Fatal("Malformed address")
	}
	port := split[1]

	extSplit := strings.Split(extAddrs[*replicaID], ":")
	if len(extSplit) != 2 {
		sugar.Fatal("Malformed address")
	}
	extPort := extSplit[1]

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
		clients = append(clients, c)
	}

	var extClients []proto.RaftClient
	for _, a := range extAddrs {
		cc, err := grpc.Dial(a, dialOpts...)
		if err != nil {
			log.Fatalf("unable to connect to host: %v", err)
		}
		c := proto.NewRaftClient(cc)
		extClients = append(extClients, c)
	}

	extListener, err := net.Listen("tcp", ":"+extPort)
	if err != nil {
		sugar.Fatal(err)
	}

	r := server.NewReplica(int64(*replicaID), clients, addrs, extClients, sugar)
	rse := server.RaftServer{R: r}

	se := grpc.NewServer()
	proto.RegisterRaftServer(se, rse)

	go func() {
		se.Serve(extListener)
	}()

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal(err)
	}

	rs := server.ReplicaServer{R: r}

	s := grpc.NewServer()
	proto.RegisterReplicaServer(s, rs)

	s.Serve(listener)
}
