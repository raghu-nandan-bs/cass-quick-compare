package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"sync/atomic"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	ratelimiter "go.uber.org/ratelimit"
)

var (
	sourceHosts      *string
	sourcePort       *int
	sourceUsername   *string
	sourcePassword   *string
	ratelimit        *int
	splitSize        *int
	targetHosts      *string
	targetPort       *int
	targetUsername   *string
	targetPassword   *string
	logLevel         *string
	keyspace         *string
	pageSize         *int
	queryTemplate    *string
	workers          *int
	consistencyLevel *string
	connections      *int
	rateLimiter      ratelimiter.Limiter
)

func init() {
	// parse args
	parseArgs()
	switch *logLevel {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	}
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	log.Debug().Msg("log level set to " + *logLevel)
	rateLimiter = ratelimiter.New(*ratelimit)
}

func parseArgs() {
	sourceHosts = flag.String("source-hosts", "localhost", "source hosts")
	sourcePort = flag.Int("source-port", 9042, "source port")
	sourceUsername = flag.String("source-username", "", "source username")
	sourcePassword = flag.String("source-password", "", "source password")
	ratelimit = flag.Int("ratelimit", 1000, "ratelimit")
	splitSize = flag.Int("split-size", 1000, "split size")
	targetHosts = flag.String("target-hosts", "localhost", "target hosts")
	targetPort = flag.Int("target-port", 9042, "target port")
	targetUsername = flag.String("target-username", "", "target username")
	targetPassword = flag.String("target-password", "", "target password")
	logLevel = flag.String("log-level", "info", "log level options: debug, info, warn, error")
	keyspace = flag.String("keyspace", "", "keyspace")
	pageSize = flag.Int("page-size", 1000, "page size")
	queryTemplate = flag.String("query-template", "", "query template")
	workers = flag.Int("workers", 4, "number of workers")
	consistencyLevel = flag.String("consistency-level", "LOCAL_QUORUM", "consistency level")
	connections = flag.Int("connections", 1, "number of connections")
	flag.Parse()
}

func main() {
	log.Info().Msg("Starting scan...")
	sourceCluster, err := newCassandraCluster(
		*sourceHosts, *sourcePort,
		*sourceUsername, *sourcePassword,
		*keyspace, *connections,
		*pageSize, *ratelimit, *consistencyLevel,
	)
	targetCluster, err := newCassandraCluster(
		*targetHosts, *targetPort,
		*targetUsername, *targetPassword,
		*keyspace, *connections,
		*pageSize, *ratelimit, *consistencyLevel,
	)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create source cluster")
	}

	runner := NewRunner(*workers)

	runner.Run()

	defer sourceCluster.close()
	defer targetCluster.close()

	var missingInSource int64
	var missingInTarget int64
	var totalSourceKeysRead int64
	var totalTargetKeysRead int64

	log.Debug().Msg("splitting ranges")
	ranges := splitRange(*splitSize)

	log.Debug().Msgf("ranges: %v", ranges)
	min := ranges[0]
	max := int64(math.MaxInt64)
	for _, max := range ranges[1:] {
		log.Debug().Msgf("creating scan job for range min: %d, max: %d", min, max)
		job := func(id, lmin, lmax int64) {

			log := log.With().Int64("id", id).Str("range", fmt.Sprintf("%v-%v", lmin, lmax)).Logger()

			log.Info().Msgf("Scanning records in range min: %d, max: %d", lmin, lmax)

			sourceIter := sourceCluster.session.Query(*queryTemplate, lmin, lmax).Iter()
			targetIter := targetCluster.session.Query(*queryTemplate, lmin, lmax).Iter()

			allSourceKeysInBatch := make(map[string]interface{})
			allTargetKeysInBatch := make(map[string]interface{})

			sourceScanComplete := false
			targetScanComplete := false

			for {
				rateLimiter.Take()
				sourceData := make(map[string]interface{})
				targetData := make(map[string]interface{})

				if sourceScanComplete && targetScanComplete {
					break
				}
				if !sourceScanComplete {
					if !sourceIter.MapScan(sourceData) {
						sourceScanComplete = true
					} else {
						key := alamgametedKeys(sourceData)
						allSourceKeysInBatch[key] = sourceData
						atomic.AddInt64(&totalSourceKeysRead, 1)
					}
				}
				if !targetScanComplete {
					if !targetIter.MapScan(targetData) {
						targetScanComplete = true
					} else {
						key := alamgametedKeys(targetData)
						allTargetKeysInBatch[key] = targetData
						atomic.AddInt64(&totalTargetKeysRead, 1)
					}
				}
			}

			for key, _ := range allSourceKeysInBatch {
				if _, ok := allTargetKeysInBatch[key]; !ok {
					missingInTarget++
					log.Warn().Msgf("Missing in target: %v", key)
				}
			}

			for key, _ := range allTargetKeysInBatch {
				if _, ok := allSourceKeysInBatch[key]; !ok {
					missingInSource++
					log.Warn().Msgf("Missing in source: %v", key)
				}
			}

		}

		runner.addTask(job, min, max)
		min = max

	}
	// jobs are done, close the workers
	runner.addTask(func( /* args are dummy */ id, x, y int64) {
		close(runner.tasks)
	}, /* args are dummy */ min, max,
	)

	runner.wg.Wait()

	log.Info().Msg("========================================")
	log.Info().Msgf("Missing in source: %d", missingInSource)
	log.Info().Msgf("Missing in target: %d", missingInTarget)
	log.Info().Msgf("Total source keys read: %d", totalSourceKeysRead)
	log.Info().Msgf("Total target keys read: %d", totalTargetKeysRead)
	log.Info().Msg("========================================")

}

// helpers
func isEqual(sourceData, targetData map[string]interface{}) bool {
	for k, v := range sourceData {
		if targetData[k] != v {
			return false
		}
	}
	return true
}

// helpers
func alamgametedKeys(toSort map[string]interface{}) string {

	amalgamatedKeys := ""

	keys := make([]string, 0, len(toSort))
	for k := range toSort {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, k := range keys {
		amalgamatedKeys += fmt.Sprintf("%v ", toSort[k])
	}
	return amalgamatedKeys
}
