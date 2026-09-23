package bloom

import (
	"fmt"
	"runtime"
	"testing"
)

var benchmarkContains bool

func BenchmarkMayContain(b *testing.B) {
	const itemCount = 10_000
	filter, err := New(itemCount, 0.01)
	if err != nil {
		b.Fatalf("New: %v", err)
	}

	keys := make([][]byte, itemCount)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "key/%020d", i)
		filter.Add(keys[i])
	}

	missing := []byte("missing/key")
	for filter.MayContain(missing) {
		missing = append(missing, 'x')
	}

	b.Run("Present", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			benchmarkContains = filter.MayContain(keys[i%len(keys)])
		}
	})

	b.Run("Absent", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			benchmarkContains = filter.MayContain(missing)
		}
	})

	b.Run("ParallelAbsent", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			var contains bool
			for pb.Next() {
				contains = filter.MayContain(missing)
			}
			runtime.KeepAlive(contains)
		})
	})
}
