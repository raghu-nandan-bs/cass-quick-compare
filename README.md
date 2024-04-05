# cass-quick-compare

comparing data between cassandra/scylla cluster

> caveat - paritioners in both clusters must be the same

### build

```
make
```

### command example

```
./cqc       \
  --target-hosts=<hosts in target-cluster>       \
  --source-hosts=<hosts in source-cluster>       \
  --keyspace=<keyspace to connect to>       \
  --source-username=<source cluster username>       \
  --target-username=<target cluster username>       \
  --source-password=<source cluster password>       \
  --target-password=<target cluster password>       \
  --log-level=info       \
  --query-template="select <pk1>,<pk2> token(pk1, pk2) from <keyspace>.<table> where token(<pk1>,<pk2>) > ? AND token(<pk1>,<pk2>) < ? ALLOW FILTERING"       \
  --ratelimit=200000       \
  --split-size=100000       \
  --connections=12       \
  --consistency-level=LOCAL_QUORUM
```

### help on command

Options available:

```
Usage of ./cqc:
  -connections int
        number of connections (default 1)
  -consistency-level string
        consistency level (default "LOCAL_QUORUM")
  -keyspace string
        keyspace
  -log-level string
        log level options: debug, info, warn, error (default "info")
  -page-size int
        page size (default 1000)
  -query-template string
        query template
  -ratelimit int
        ratelimit (default 1000)
  -source-hosts string
        source hosts (default "localhost")
  -source-password string
        source password
  -source-port int
        source port (default 9042)
  -source-username string
        source username
  -split-size int
        split size (default 1000)
  -target-hosts string
        target hosts (default "localhost")
  -target-password string
        target password
  -target-port int
        target port (default 9042)
  -target-username string
        target username
  -workers int
        number of workers (default 4)
```
