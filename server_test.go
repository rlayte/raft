package raft

import (
	"fmt"
	"testing"
	"time"
)

func createCluster(name string, n int) []*Server {
	cluster := []*Server{}

	for i := 0; i < n; i++ {
		id := fmt.Sprintf("%d", i)
		cluster = append(cluster, NewServer(id, name, n))
	}

	return cluster
}

func destroyCluster(cluster []*Server) {
	for _, server := range cluster {
		server.kill()
	}
}

func TestNewServer(t *testing.T) {
	cluster := createCluster("basic", 1)
	defer destroyCluster(cluster)

	for index, server := range cluster {
		if server.Role != Follower {
			t.Error("Servers should start as followers")
		}

		if server.Term != 0 {
			t.Error("Servers should start at term 0")
		}

		if server.host() != fmt.Sprintf("/var/tmp/raft-basic-%d", index) {
			t.Error("Servers should set the correct host")
		}
	}
}

func TestFirstElection(t *testing.T) {
	cluster := createCluster("election", 5)
	defer destroyCluster(cluster)

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

func TestAppendEntries(t *testing.T) {
	cluster := createCluster("append", 5)
	defer destroyCluster(cluster)

	time.Sleep(time.Second)

	command := Command{}
	client := Client{cluster[0].host()}
	client.Execute(command)

	time.Sleep(time.Second)

	for _, server := range cluster {
		if len(server.Log) == 0 {
			t.Error("No commands appended to log")
			continue
		}

		firstEntry := server.Log[0]

		if firstEntry.Term != server.Term {
			t.Error("Entry should share the same term as primary")
		}

		if firstEntry.Command != command {
			t.Error("First command should be appended")
		}
	}
}
