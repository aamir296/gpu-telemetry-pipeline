package shard

import "testing"

func TestOwnerDeterministicAndBoundedMovement(t *testing.T) {
	members := []string{"one", "two", "three"}
	counts := map[string]int{}
	before := map[string]string{}
	for i := 0; i < 1000; i++ {
		key := string(rune(i))
		owner := Owner("gpu", key, members)
		if owner != Owner("gpu", key, []string{"three", "one", "two"}) {
			t.Fatal("member order changed ownership")
		}
		counts[owner]++
		before[key] = owner
	}
	for _, member := range members {
		if counts[member] < 200 {
			t.Fatalf("unexpectedly skewed ownership: %#v", counts)
		}
	}
	moved := 0
	for key, old := range before {
		if current := Owner("gpu", key, []string{"one", "two"}); current != old {
			moved++
			if old != "three" {
				t.Fatalf("key moved between surviving members: %q -> %q", old, current)
			}
		}
	}
	if moved == 0 {
		t.Fatal("removing a member moved no keys")
	}
	if Owner("gpu", "x", nil) != "" {
		t.Fatal("empty membership should have no owner")
	}
}
