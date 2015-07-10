package raft

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"time"
)

const (
	Follower  ServerState = "Follower"
	Leader    ServerState = "Leader"
	Candidate ServerState = "Candidate"

	HeartbeatInterval int = 10
	ElectionTimeout   int = 150
)

type ServerState string

func call(srv string, rpcname string, args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	return err == nil
}

type Server struct {
	Id              string
	Role            ServerState
	Term            int
	LastContact     time.Time
	Cluster         []Server
	ClusterSize     int
	VotedFor        string
	ElectionTimeout time.Duration
}

func (s *Server) startElection() {
	s.Role = Candidate
	s.Term = 1

	votes := 0

	for _, server := range s.Cluster {
		args := VoteArgs{s.Term, s.Id}
		reply := VoteReply{}
		call(server.host(), "Server.RequestVote", &args, &reply)

		if reply.Granted {
			votes++
		}
	}

	if votes > s.ClusterSize/2 {
		s.Role = Leader
	}
}

func (s *Server) updateFollowers() {
	for _, server := range s.Cluster {
		args := AppendEntriesArgs{s.Term, s.Id}
		reply := AppendEntriesReply{}
		call(server.host(), "Server.AppendEntries", &args, &reply)
	}
}

func (s *Server) host() string {
	return "/var/tmp/raft-" + s.Id
}

func (s *Server) leaderDead() bool {
	return time.Since(s.LastContact) > s.ElectionTimeout*time.Millisecond
}

func (s *Server) heartbeat() {
	if s.Role == Leader {
		s.updateFollowers()
	}

	if s.leaderDead() && s.Role == Follower {
		s.startElection()
	}
}

func (s *Server) startHeartbeat() {
	for {
		s.heartbeat()
		time.Sleep(time.Millisecond * time.Duration(HeartbeatInterval))
	}
}

func (s *Server) startRPC() {
	rpcs := rpc.NewServer()
	rpcs.Register(s)

	os.Remove(s.host())
	l, e := net.Listen("unix", s.host())
	if e != nil {
		log.Fatal("listen error: ", e)
	}

	for {
		conn, err := l.Accept()

		if err != nil {
			log.Println("Error", err)
		}

		go rpcs.ServeConn(conn)
	}
}

func (s *Server) configureCluster() {
	for i := 0; i < s.ClusterSize; i++ {
		if id := fmt.Sprintf("%d", i); id != s.Id {
			s.Cluster = append(s.Cluster, Server{Id: id})
		}
	}
}

type VoteArgs struct {
	Term int
	Id   string
}

type VoteReply struct {
	Term    int
	Granted bool
}

func (s *Server) RequestVote(args *VoteArgs, reply *VoteReply) error {
	if args.Term < s.Term {
		reply.Term = s.Term
		reply.Granted = false
	} else {
		s.Term = args.Term

		if s.VotedFor == "" || s.VotedFor == args.Id {
			s.VotedFor = args.Id
			reply.Granted = true
			reply.Term = s.Term
		}
	}

	return nil
}

type AppendEntriesArgs struct {
	Term int
	Id   string
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (s *Server) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	if args.Term < s.Term {
		reply.Term = s.Term
		reply.Success = false
	} else {
		s.Term = args.Term
		s.Role = Follower
		s.LastContact = time.Now()

		reply.Term = s.Term
		reply.Success = true
	}

	return nil
}

func NewServer(id string, clusterSize int) (s *Server) {
	s = &Server{
		Id:          id,
		Role:        Follower,
		Term:        0,
		Cluster:     []Server{},
		LastContact: time.Now(),
		ClusterSize: clusterSize,
	}

	s.ElectionTimeout = time.Duration(ElectionTimeout + rand.Intn(ElectionTimeout))
	s.configureCluster()

	go s.startHeartbeat()
	go s.startRPC()

	return
}
