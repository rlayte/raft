package raft

import (
	"fmt"
	"testing"
	"time"
)

type KVStore struct {
	*Server
	data map[string]interface{}
}

func (s *KVStore) commit(command Command) interface{} {
	switch command.Operation {
	case "PUT":
		s.data[command.Key] = command.Value
	case "GET":
		return s.data[command.Key]
	}

	return nil
}

func createCluster(name string, n int) []*KVStore {
	cluster := []*KVStore{}

	for i := 0; i < n; i++ {
		id := fmt.Sprintf("%d", i)
		store := &KVStore{NewServer(id, name, n), map[string]interface{}{}}
		store.Commit = store.commit
		cluster = append(cluster, store)
	}

	return cluster
}

func destroyCluster(cluster []*KVStore) {
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

	time.Sleep(time.Duration(ElectionTimeout*2) * time.Millisecond)

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

func TestLogReplication(t *testing.T) {
	cluster := createCluster("append", 5)
	defer destroyCluster(cluster)

	time.Sleep(time.Duration(ElectionTimeout*2) * time.Millisecond)

	command := Command{"PUT", "Foo", "Bar"}
	client := Client{cluster[0].host()}
	client.Execute(command)

	time.Sleep(time.Duration(HeartbeatInterval*2) * time.Millisecond)

	for _, server := range cluster {
		if len(server.Log) == 0 {
			t.Error("No commands appended to log")
			continue
		}

		firstEntry := server.Log[1]

		if firstEntry.Term != server.Term {
			t.Error("Entry should share the same term as primary")
		}

		if firstEntry.Command.Key != command.Key {
			t.Error("First command should be appended")
		}
	}
}

func checkStateMachine(t *testing.T, client *Client, cluster []*KVStore, cases map[string]string) {
	for k, v := range cases {
		command := Command{"PUT", k, v}
		client.Execute(command)

		time.Sleep(time.Duration(HeartbeatInterval*3) * time.Millisecond)

		for _, server := range cluster {
			if server.data[k] != v {
				t.Error(k, "should equal", v, "not", server.data[k], server.data)
			}
		}
	}
}

func TestStateReplication(t *testing.T) {
	cluster := createCluster("append", 5)
	defer destroyCluster(cluster)

	time.Sleep(time.Duration(ElectionTimeout*2) * time.Millisecond)

	client := Client{cluster[0].host()}

	cases := map[string]string{
		"Foo": "Bar",
		"Bar": "Foo",
	}

	checkStateMachine(t, &client, cluster, cases)

	updates := map[string]string{
		"Foo": "Baz",
		"Bar": "Baz",
	}

	checkStateMachine(t, &client, cluster, updates)
}
