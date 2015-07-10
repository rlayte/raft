package raft

import (
	"fmt"
	"testing"
	"time"
)

func TestNewServer(t *testing.T) {
	s := NewServer("0", 1)

	if s.Role != Follower {
		t.Error("Servers should start as followers")
	}

	if s.Term != 0 {
		t.Error("Servers should start at term 0")
	}
}

func TestFirstElection(t *testing.T) {
	var cluster [5]*Server
	n := len(cluster)

	for i := 0; i < n; i++ {
		id := fmt.Sprintf("%d", i)
		cluster[i] = NewServer(id, n)
	}

	time.Sleep(time.Second)

	leaders := 0
	followers := 0

	for _, server := range cluster {
		if server.Role == Leader {
			leaders++
		}

		if server.Role == Follower {
			followers++
		}
	}

	if leaders != 1 {
		t.Error("There should be one leader", leaders)
	}

	if followers != 4 {
		t.Error("There should be 4 followers", followers)
	}
}
