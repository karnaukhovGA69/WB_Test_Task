package stoplist

import (
	"fmt"
	"testing"
)

func BenchmarkStopListContains(b *testing.B) {
	words := make([]string, 10_000)
	for i := range words {
		words[i] = fmt.Sprintf("blocked query %05d", i)
	}

	manager := NewManager(words)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = manager.Contains(fmt.Sprintf("blocked query %05d", i%len(words)))
	}
}
