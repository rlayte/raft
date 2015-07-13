package raft

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
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

type Vote struct {
	Id   string
	Time time.Time
}

type Server struct {
	Id              string
	ClusterId       string
	Role            ServerState
	Term            int
	Log             []Command
	LastContact     time.Time
	Cluster         []Server
	ClusterSize     int
	VotedFor        *Vote
	ElectionTimeout time.Duration
	voting          *sync.Mutex
	dead            bool
}

func (s *Server) startElection() {
	s.Role = Candidate
	s.Term = 1
	s.VotedFor = &Vote{s.Id, time.Now()}

	votes := 1

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
	return fmt.Sprintf("/var/tmp/raft-%s-%s", s.ClusterId, s.Id)
}

func (s *Server) leaderDead() bool {
	return time.Since(s.LastContact) > s.ElectionTimeout*time.Millisecond
}

func (s *Server) voteInvalid() bool {
	return s.VotedFor == nil || time.Since(s.VotedFor.Time) > s.ElectionTimeout*time.Millisecond
}

func (s *Server) validCandidate() bool {
	return s.Role == Follower && s.leaderDead() && s.voteInvalid()
}

func (s *Server) heartbeat() {
	if s.Role == Leader {
		s.updateFollowers()
	}

	if s.validCandidate() {
		s.startElection()
	}
}

func (s *Server) startHeartbeat() {
	for !s.dead {
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

func (s *Server) kill() {
	s.dead = true
}

func (s *Server) configureCluster() {
	for i := 0; i < s.ClusterSize; i++ {
		if id := fmt.Sprintf("%d", i); id != s.Id {
			s.Cluster = append(s.Cluster, Server{Id: id, ClusterId: s.ClusterId})
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
		s.voting.Lock()
		if s.VotedFor == nil || s.VotedFor.Id == args.Id {
			s.Term = args.Term
			s.VotedFor = &Vote{args.Id, time.Now()}
			reply.Granted = true
			reply.Term = s.Term

		}
		s.voting.Unlock()
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

type Command struct {
}

type ExecuteCommandArgs struct {
	Command Command
}

type ExecuteCommandReply struct {
	Success bool
}

func (s *Server) Execute(args *ExecuteCommandArgs, reply *ExecuteCommandReply) error {
	s.Log = append(s.Log, args.Command)
	return nil
}

func NewServer(id string, clusterId string, clusterSize int) (s *Server) {
	s = &Server{
		Id:          id,
		ClusterId:   clusterId,
		Role:        Follower,
		Term:        0,
		Log:         []Command{},
		Cluster:     []Server{},
		LastContact: time.Now(),
		ClusterSize: clusterSize,
		voting:      &sync.Mutex{},
	}

	s.ElectionTimeout = time.Duration(ElectionTimeout + rand.Intn(ElectionTimeout))
	s.configureCluster()

	go s.startHeartbeat()
	go s.startRPC()

	return
}
