package store

import (
	"context"
	"fmt"
	"testing"

	"github.com/aamir296/gpu-telemetry-pipeline/internal/model"
)

func BenchmarkAppendDurableBatch100(b *testing.B) {
	store, err := Open(b.TempDir(), 8, 64<<20, 1<<20)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		batch := make([]model.Telemetry, 100)
		for index := range batch {
			batch[index] = event(fmt.Sprintf("GPU-%d", index%8), fmt.Sprintf("%d-%d", iteration, index))
		}
		if _, err := store.Append(ctx, batch); err != nil {
			b.Fatal(err)
		}
	}
}
