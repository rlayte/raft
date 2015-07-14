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

	WrongServerError     RaftError = "WrongServerError"
	LogInconsistentError RaftError = "LogInconsistentError"
	TermOutdatedError    RaftError = "TermOutdatedError"
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
	NextIndex       map[string]int
	MatchIndex      map[string]int
	VotedFor        *Vote
	ElectionTimeout time.Duration
	Commit          CommitFunc
	voting          *sync.Mutex
	dead            bool
}

func (s *Server) becomeLeader() {
	s.Role = Leader

	for _, node := range s.Cluster {
		lastEntry := s.Log[len(s.Log)-1]
		s.NextIndex[node.host()] = lastEntry.Index + 1
		s.MatchIndex[node.host()] = 0
	}
}

func (s *Server) startElection() {
	s.Role = Candidate
	s.Term = 1
	s.VotedFor = &Vote{s.Id, time.Now()}

	votes := 1
	lastEntry := s.Log[len(s.Log)-1]

	for _, server := range s.Cluster {
		args := VoteArgs{s.Term, s.Id, lastEntry.Index, lastEntry.Term}
		reply := VoteReply{}
		call(server.host(), "Server.RequestVote", &args, &reply)

		if reply.Granted {
			votes++
		}
	}

	if votes > s.ClusterSize/2 {
		s.becomeLeader()
	}
}

func (s *Server) callAppendEntries(server Server, entries []LogEntry, prevEntry LogEntry) (bool, AppendEntriesReply) {
	args := AppendEntriesArgs{s.Term, s.host(), entries, s.CommitIndex, prevEntry.Index, prevEntry.Term}
	reply := AppendEntriesReply{}
	ok := call(server.host(), "Server.AppendEntries", &args, &reply)
	return ok, reply
}

func (s *Server) updateFollowers() bool {
	var failures int

	for _, server := range s.Cluster {
		prevEntry := s.Log[s.NextIndex[server.host()]]
		entries := s.Log[s.NextIndex[server.host()]:]
		ok, reply := s.callAppendEntries(server, entries, prevEntry)

		for reply.Error == LogInconsistentError {
			s.NextIndex[server.host()] -= 1
			entries = s.Log[s.NextIndex[server.host()]:]
			ok, reply = s.callAppendEntries(server, entries, prevEntry)
		}

		if !ok || !reply.Success {
			failures++
		} else {
			lastEntry := s.Log[len(s.Log)-1]
			s.NextIndex[server.host()] = lastEntry.Index + 1
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
		for _, server := range s.Cluster {
			lastEntry := s.Log[len(s.Log)-1]
			s.callAppendEntries(server, []LogEntry{}, lastEntry)
		}
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

func (s *Server) updateCommitIndex(leaderCommit int) {
	if leaderCommit > s.CommitIndex {
		lastEntry := s.Log[len(s.Log)-1]
		if leaderCommit < lastEntry.Index {
			s.CommitIndex = leaderCommit
		} else {
			s.CommitIndex = lastEntry.Index
		}
	}
}

func (s *Server) kill() {
	s.dead = true
}

func (s *Server) configureCluster() {
	for i := 0; i < s.ClusterSize; i++ {
		if id := fmt.Sprintf("%d", i); id != s.Id {
			node := Server{Id: id, ClusterId: s.ClusterId}
			s.Cluster = append(s.Cluster, node)
			s.NextIndex[node.host()] = 0
			s.MatchIndex[node.host()] = 0
		}
	}
}

type VoteArgs struct {
	Term         int
	Id           string
	LastLogIndex int
	LastLogTerm  int
}

type VoteReply struct {
	Term    int
	Granted bool
}

func (s *Server) uptoDate(index int, term int) bool {
	if term != s.Term {
		return term > s.Term
	} else {
		lastEntry := s.Log[len(s.Log)-1]
		return index >= lastEntry.Index
	}
}

func (s *Server) RequestVote(args *VoteArgs, reply *VoteReply) error {
	if args.Term < s.Term {
		reply.Term = s.Term
		reply.Granted = false
	} else {
		s.voting.Lock()
		if (s.VotedFor == nil || s.VotedFor.Id == args.Id) && s.uptoDate(args.LastLogIndex, args.LastLogTerm) {
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
	Error   RaftError
}

func (s *Server) logInconsistent(index int, term int) bool {
	if index >= len(s.Log) {
		return false
	}

	return s.Log[index].Term != term
}

func (s *Server) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	if args.Term < s.Term {
		reply.Term = s.Term
		reply.Success = false
		reply.Error = TermOutdatedError
	} else if s.logInconsistent(args.PrevLogIndex, args.PrevLogTerm) {
		log.Println("Mismatch index/term")
		reply.Success = false
		reply.Error = LogInconsistentError
	} else {
		s.Term = args.Term
		s.Leader = args.Id
		s.Role = Follower
		s.LastContact = time.Now()

		if args.PrevLogIndex < len(s.Log) {
			s.Log = s.Log[:args.PrevLogIndex+1]
		}

		s.Log = append(s.Log, args.Entries...)
		s.updateCommitIndex(args.LeaderCommit)

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
	safe := s.updateFollowers()

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
		NextIndex:   map[string]int{},
		MatchIndex:  map[string]int{},
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
