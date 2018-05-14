package main

import (
	"flag"
	"log"
	"os"

	"github.com/anthuang/go-raft/client"
)

var (
	configPath  = flag.String("config", "", "path to config")
	prefReplica = flag.Int("id", -1, "id of preferred replica to first contact")
)

func main() {
	flag.Parse()
	if *configPath == "" || *prefReplica == -1 {
		flag.Usage()
		os.Exit(2)
	}

	c := client.NewClient(*configPath, *prefReplica)

	c.Put("Hello", "World")
	log.Println("Hello", c.Get("Hello"))
}
