package main

import (
	"math"

	"github.com/gocql/gocql"
	"github.com/rs/zerolog/log"
)

type cassandraCluster struct {
	session     *gocql.Session
	hosts       string
	port        int
	username    string
	password    string
	keyspace    string
	connections int
	ratelimit   int
}

func newCassandraCluster(hosts string, port int, username string, password string,
	keyspace string, connections int, pageSize int, ratelimit int, consistencyLevel string) (*cassandraCluster, error) {
	session, err := connect(hosts, port, username, password, keyspace, connections, pageSize, consistencyLevel)

	if err != nil {
		return nil, err
	}

	return &cassandraCluster{
		session:     session,
		hosts:       hosts,
		port:        port,
		username:    username,
		password:    password,
		keyspace:    keyspace,
		connections: connections,
		ratelimit:   ratelimit,
	}, nil
}

func connect(hosts string, port int, username string, password string,
	keyspace string, connections int, pageSize int, consistencyLevel string) (*gocql.Session, error) {
	cluster := gocql.NewCluster(hosts)
	cluster.Port = port
	cluster.Keyspace = keyspace
	cluster.NumConns = connections
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: username,
		Password: password,
	}
	cluster.Consistency = gocql.ParseConsistency(consistencyLevel)
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	session.SetPageSize(pageSize)

	return session, nil
}

// close the session
func (cc *cassandraCluster) close() {
	cc.session.Close()
}

func splitRange(numSplits int) []int64 {
	log.Debug().Msg("splitting ranges between -2^63 and 2^63-1")
	min := float64(math.MinInt64)
	max := float64(math.MaxInt64)
	splitSize := int64((max - min) / float64(numSplits))
	splits := make([]int64, 0, numSplits)
	splits = append(splits, int64(min))
	for i := 1; ; i++ {
		nextMax := min + float64(float64(i)*float64(splitSize))
		if nextMax >= max {
			splits = append(splits, math.MaxInt64)
			break
		}
		splits = append(splits, int64(nextMax-1))
	}
	return splits
}
