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

	WrongServerError RaftError = "WrongServerError"
)

type ServerState string
type RaftError string

func call(srv string, rpcname string, args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	return err == nil
}

type Command struct {
	Operation string
	Key       string
	Value     interface{}
}

type Vote struct {
	Id   string
	Time time.Time
}

type LogEntry struct {
	Index   int
	Term    int
	Command Command
}

type CommitFunc func(Command) interface{}

type Server struct {
	Id              string
	ClusterId       string
	Role            ServerState
	Leader          string
	Term            int
	Log             []LogEntry
	LastContact     time.Time
	LastApplied     int
	CommitIndex     int
	Cluster         []Server
	ClusterSize     int
	VotedFor        *Vote
	ElectionTimeout time.Duration
	Commit          CommitFunc
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

func (s *Server) updateFollowers(entries []LogEntry) bool {
	var failures int

	for _, server := range s.Cluster {
		lastEntry := s.Log[len(s.Log)-1]
		args := AppendEntriesArgs{s.Term, s.host(), entries, s.CommitIndex, lastEntry.Index, lastEntry.Term}
		reply := AppendEntriesReply{}
		ok := call(server.host(), "Server.AppendEntries", &args, &reply)

		if !ok || !reply.Success {
			failures++
		}
	}

	return failures <= s.ClusterSize/2
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
		s.updateFollowers([]LogEntry{})
	}

	for s.LastApplied < s.CommitIndex {
		i := s.LastApplied + 1
		if i > len(s.Log)-1 {
		} else {
			s.Commit(s.Log[i].Command)
			s.LastApplied = s.Log[i].Index
		}
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
	Term         int
	Id           string
	Entries      []LogEntry
	LeaderCommit int
	PrevLogIndex int
	PrevLogTerm  int
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
		s.Leader = args.Id
		s.Role = Follower
		s.LastContact = time.Now()
		s.Log = append(s.Log, args.Entries...)

		if args.LeaderCommit > s.CommitIndex {
			lastEntry := s.Log[len(s.Log)-1]
			if args.LeaderCommit < lastEntry.Index {
				s.CommitIndex = args.LeaderCommit
			} else {
				s.CommitIndex = lastEntry.Index
			}
		}

		reply.Term = s.Term
		reply.Success = true
	}

	return nil
}

type ExecuteCommandArgs struct {
	Command Command
}

type ExecuteCommandReply struct {
	Success bool
	Error   RaftError
	Leader  string
	Update  interface{}
}

func (s *Server) Execute(args *ExecuteCommandArgs, reply *ExecuteCommandReply) error {
	if s.Role != Leader {
		reply.Error = WrongServerError
		reply.Success = false
		reply.Leader = s.Leader
		return nil
	}

	entry := LogEntry{len(s.Log), s.Term, args.Command}
	s.Log = append(s.Log, entry)
	safe := s.updateFollowers([]LogEntry{entry})

	if safe {
		reply.Update = s.Commit(entry.Command)
		s.LastApplied = entry.Index
		s.CommitIndex = entry.Index
	}

	return nil
}

func NewServer(id string, clusterId string, clusterSize int) (s *Server) {
	s = &Server{
		Id:          id,
		ClusterId:   clusterId,
		Role:        Follower,
		Term:        0,
		Log:         []LogEntry{LogEntry{}},
		Cluster:     []Server{},
		LastContact: time.Now(),
		ClusterSize: clusterSize,
		voting:      &sync.Mutex{},
	}

	s.Commit = func(command Command) interface{} {
		log.Println(s.host(), "Committing command to state machine", command)
		return true
	}

	s.ElectionTimeout = time.Duration(ElectionTimeout + rand.Intn(ElectionTimeout))
	s.configureCluster()

	go s.startHeartbeat()
	go s.startRPC()

	return
}
