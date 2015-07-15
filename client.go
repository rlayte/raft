package raft

import (
	"log"
	"math/rand"
	"net/rpc"
)

type Client struct {
	cluster []*KVStore
	Primary string
}

func (c *Client) call(rpcname string, args interface{}, reply interface{}) bool {
	conn, errx := rpc.Dial("unix", c.Primary)
	if errx != nil {
		return false
	}
	defer conn.Close()

	err := conn.Call(rpcname, args, reply)
	return err == nil
}

func (c *Client) Execute(command Command) interface{} {
	args := ExecuteCommandArgs{command}
	reply := ExecuteCommandReply{}
	ok := c.call("Server.Execute", &args, &reply)

	if !ok {
		newPrimary := c.cluster[rand.Intn(len(c.cluster))].host()
		log.Println("Client error: changing primary", c.Primary, newPrimary)
		c.Primary = newPrimary
		ok = c.call("Server.Execute", &args, &reply)
	}

	if reply.Error == WrongServerError {
		log.Println("Wrong primary:", c.Primary, reply.Leader)
		c.Primary = reply.Leader
		ok = c.call("Server.Execute", &args, &reply)
	}

	log.Println("Client executed", command, c.Primary)

	return reply.Update
}

func NewClient(cluster []*KVStore) *Client {
	return &Client{cluster, cluster[0].host()}
}
