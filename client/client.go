package client

import (
	"bufio"
	"context"
	"log"
	"os"
	"strings"
	"time"

	"github.com/anthuang/go-raft/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Client implements a client to talk to Raft servers
type Client struct {
	logger      *zap.SugaredLogger
	prefReplica int
	raftClients []proto.RaftClient
}

// NewClient returns a new client
func NewClient(config string, prefReplica int) *Client {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	sugar := logger.Sugar()

	extAddrs := []string{}
	f, err := os.Open(config)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	scan := bufio.NewScanner(f)
	for scan.Scan() {
		fields := strings.Fields(scan.Text())
		extAddrs = append(extAddrs, fields[1])
	}

	var dialOpts []grpc.DialOption
	dialOpts = append(dialOpts, grpc.WithInsecure())
	dialOpts = append(dialOpts, grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 3 * time.Second}))

	var extClients []proto.RaftClient
	for _, a := range extAddrs {
		cc, err := grpc.Dial(a, dialOpts...)
		if err != nil {
			log.Fatalf("unable to connect to host: %v", err)
		}
		c := proto.NewRaftClient(cc)
		extClients = append(extClients, c)
	}

	c := &Client{
		logger:      sugar,
		prefReplica: prefReplica,
		raftClients: extClients,
	}

	return c
}

// Get returns the value with key
func (c *Client) Get(key string) string {
	i := c.prefReplica
	for {
		resp, err := c.raftClients[i].Get(context.Background(), &proto.GetReq{
			Key: key,
		})
		if err != nil {
			c.logger.Errorf("Error during GET: %s", err)

			i = (i + 1) % len(c.raftClients)
			if i == c.prefReplica {
				// Wait if tried all replicas already
				time.Sleep(200 * time.Millisecond)
			}
		} else {
			return resp.Value
		}
	}
}

// Put updates the value with key
func (c *Client) Put(key string, value string) {
	i := c.prefReplica
	for {
		_, err := c.raftClients[i].Put(context.Background(), &proto.PutReq{
			Key:   key,
			Value: value,
		})
		if err != nil {
			c.logger.Errorf("Error during PUT: %s", err)

			i = (i + 1) % len(c.raftClients)
			if i == c.prefReplica {
				// Wait if tried all replicas already
				time.Sleep(200 * time.Millisecond)
			}
		} else {
			return
		}
	}
}
