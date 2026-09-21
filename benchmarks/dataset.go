package benchmarks

import (
	"encoding/binary"
	"flag"
	"fmt"
)

const (
	benchmarkDatasetVersion = 1
	defaultBenchmarkSeed    = uint64(0x5eed687970686164)
	valueSeedDomain         = uint64(0x76616c75652d7631)
	shuffleSeedDomain       = uint64(0x73687566666c6531)
)

var benchmarkSeed = flag.Uint64(
	"seed",
	defaultBenchmarkSeed,
	"deterministic benchmark dataset seed",
)

type benchmarkRecord struct {
	key   benchmarkKey
	value []byte
}

// splitMix64 is a small, explicitly defined generator whose output remains
// stable independently of Go's standard-library random-number generators.
type splitMix64 struct {
	state uint64
}

func (generator *splitMix64) next() uint64 {
	generator.state += 0x9e3779b97f4a7c15
	value := generator.state
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func makeDataset(count, size int, seed uint64) []benchmarkRecord {
	records := make([]benchmarkRecord, count)
	for index := range records {
		text := fmt.Sprintf("key/%020d", index)
		records[index] = benchmarkRecord{
			key: benchmarkKey{
				text: text,
				raw:  []byte(text),
			},
			value: makeDeterministicBytes(size, seed, index),
		}
	}
	return records
}

func makeDeterministicBytes(size int, seed uint64, index int) []byte {
	value := make([]byte, size)
	generator := splitMix64{
		state: seed ^ valueSeedDomain ^ uint64(index),
	}
	for offset := 0; offset < len(value); {
		var encoded [8]byte
		binary.LittleEndian.PutUint64(encoded[:], generator.next())
		offset += copy(value[offset:], encoded[:])
	}
	return value
}

func datasetKeys(records []benchmarkRecord) []benchmarkKey {
	keys := make([]benchmarkKey, len(records))
	for index := range records {
		keys[index] = records[index].key
	}
	return keys
}

func shuffledKeys(keys []benchmarkKey, seed uint64) []benchmarkKey {
	shuffled := append([]benchmarkKey(nil), keys...)
	generator := splitMix64{state: seed ^ shuffleSeedDomain}
	for index := len(shuffled) - 1; index > 0; index-- {
		other := int(generator.next() % uint64(index+1))
		shuffled[index], shuffled[other] = shuffled[other], shuffled[index]
	}
	return shuffled
}
