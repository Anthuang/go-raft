# go-raft
This is a simple implementation of Raft built with Go and gRPC, aimed to help me learn through implementation. A GET and PUT interface is exposed externally.

Most of Raft functionality is implemented and tested, but small parts may be missing. Many improvements can be made, such as changing the backend storage (currently the key-value store is in memory). The code could also be cleaned up and more tests could be written (concurrency tests could be more reliable, currently I am not completely sure they are correct). Given time, I would try to accomplish these improvements.

## Usage

### Config
A sample config is given (config.txt) using localhost. Each line in the config is a new replica, and each line has a internal address and an external address, respectively.

### Server
The server takes in two arguments, config path and replica id. The server will wait until all replicas are started before initialization is complete.

```
$ go build -o bin/server cmd/server/server.go
$ ./bin/server -config=config.txt -id=0
```

### Client
The client can be used by creating a client object using `client.NewClient(...)`. The client object requires two arguments, config path and preferred replica id. The preferred replica id is the replica that the client will contact first on all requests. This is usually the closest replica.

An example usage is given in cmd/client/example_client.go.

```
$ go build -o bin/client cmd/client/example_client.go
$ ./bin/client -config=config.txt -id=0
```
