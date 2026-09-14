package clock

import (
	"testing"
	"time"
)

func TestClocksReturnUTC(t *testing.T) {
	if (System{}).Now().Location() != time.UTC {
		t.Fatal("system clock is not UTC")
	}
	input := time.Date(2024, 1, 2, 3, 4, 5, 0, time.FixedZone("other", 3600))
	if got := (Fixed{Time: input}).Now(); !got.Equal(input) || got.Location() != time.UTC {
		t.Fatalf("unexpected fixed clock %v", got)
	}
}
