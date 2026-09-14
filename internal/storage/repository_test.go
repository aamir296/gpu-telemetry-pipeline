package storage

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestCursorRoundTripAndValidation(t *testing.T) {
	want := Cursor{Version: 1, Partition: 3, Watermark: 99, Observed: time.Unix(100, 200).UTC(), Sequence: 17}
	encoded, err := encodeCursor(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeCursor(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("got %+v want %+v", got, want)
	}
	for _, input := range []string{"not-base64!", "e30"} {
		if _, err := decodeCursor(input); err == nil {
			t.Fatalf("expected %q to fail", input)
		}
	}
}

func TestOpenAndMigrateRejectInvalidURL(t *testing.T) {
	if _, err := Open(context.Background(), "://invalid"); err == nil {
		t.Fatal("expected Open error")
	}
	err := Migrate(context.Background(), "://invalid")
	if err == nil || !strings.Contains(err.Error(), "connect for migration") {
		t.Fatalf("unexpected migration error %v", err)
	}
}
