package raft

import "net/rpc"

type Client struct {
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

func (c *Client) Execute(command Command) bool {
	args := ExecuteCommandArgs{Command{}}
	reply := ExecuteCommandReply{}
	return c.call("Server.Execute", &args, &reply)
}
