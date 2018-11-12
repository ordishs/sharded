package sharded

import (
	"testing"
)

func TestMod(t *testing.T) {
	expected := []int{0, 1, 2, 3, 0}

	for i := 0; i < 5; i++ {
		a := i % 4
		if a != expected[i] {
			t.Errorf("%d mod 4 = %d. Expected %d, got %d", i, a, expected[i], a)
		}
	}
}

func TestShardWithObjects(t *testing.T) {
	keys := []interface{}{"another string", "444", true, false, []interface{}{"a"}}
	expected := []int{1, 3, 1, 2, 0}

	for i := 0; i < len(keys); i++ {
		h := getShardNumber(keys[i], 4)

		if h != expected[i] {
			t.Errorf("%d: Expected %d, got %d", i, expected[i], h)
		}
	}
}

func TestShardInts(t *testing.T) {
	// keys := []interface{}{0, 1, 2, 3, 1000, "nother string", 1, 2, 3, 4, 5, 6, 7, 8, 9, "33", "444", true, false, []interface{}{"a"}}

	// for _, k := range keys {
	// 	h := getShardNumber(k, 4)
	// 	t.Logf("shard = %d", h)
	// }

	expected := []int{3, 2, 1, 0, 3, 2, 1, 0, 3, 2}
	for i := 0; i < 10; i++ {
		h := getShardNumber(i, 4)

		if h != expected[i] {
			t.Errorf("Expected %d, got %d", expected[i], h)
		}
	}
}

func TestSetCountAndGet(t *testing.T) {
	m := New(4)
	m.Set("name", "simon")

	c := m.Count()
	if c != 1 {
		t.Errorf("Expected a count of 1, got %d", c)
	}

	n, ok := m.Get("name")

	if ok == false {
		t.Errorf("Expected true and received false")
	}

	if n != "simon" {
		t.Errorf("Excepted 'simon', got %s", n)
	}
}

func TestRemove(t *testing.T) {
	m := New(4)
	m.Set("name", "simon")

	c := m.Count()
	if c != 1 {
		t.Errorf("Expected a count of 1, got %d", c)
	}

	m.Remove("name")

	c2 := m.Count()
	if c2 != 0 {
		t.Errorf("Expected a count of 0, got %d", c2)
	}

	n, ok := m.Get("name")

	if ok == true {
		t.Errorf("Expected false and received true")
	}

	if n != nil {
		t.Errorf("Excepted nil, got %v", n)
	}
}
func TestIter(t *testing.T) {
	m := New(4)
	m.Set("name1", "simon1")
	m.Set("name2", "simon2")
	m.Set("name3", "simon3")
	m.Set("name4", "simon4")

	c := m.Count()
	if c != 4 {
		t.Errorf("Expected a count of 4, got %d", c)
	}

	i := 0
	expectedKeys := []string{"name2", "name1", "name4", "name3"}
	expectedValues := []string{"simon2", "simon1", "simon4", "simon3"}

	for tuple := range m.Iter() {

		if tuple.Key != expectedKeys[i] {
			t.Errorf("Expected %s, got %s", expectedKeys[i], tuple.Key)
		}

		if tuple.Val != expectedValues[i] {
			t.Errorf("Expected %s, got %s", expectedValues[i], tuple.Val)
		}

		i++
	}
}

func TestReset(t *testing.T) {
	m := New(4)
	m.Set("name", "simon")
	m.Update("name", func(oldVal VT) VT {
		return "bob"
	})

	r, _ := m.Get("name")
	if r != "bob" {
		t.Errorf("Expected bob, got %s", r)
	}
}

func TestResetIter(t *testing.T) {
	m := New(4)
	m.Set("name1", "simon1")
	m.Set("name2", "simon2")
	m.Set("name3", "simon3")
	m.Set("name4", "simon4")
	for tuple := range m.Iter() {
		m.Update(tuple.Key, func(oldVal VT) VT {
			if oldVal == "simon1" {
				return "bob1"
			}
			return "george"
		})
	}

	for tuple := range m.Iter() {
		if tuple.Key == "name1" && tuple.Val != "bob1" {
			t.Errorf("Expected bob1, got %s", tuple.Val)
		}

		if tuple.Key != "name1" && tuple.Val != "george" {
			t.Errorf("Expected george, got %s", tuple.Val)
		}
	}
}

func TestStats(t *testing.T) {
	type stat struct {
		workerGroup string
		good        int
		bad         int
	}

	m := New(4)

	m.Set("plott", stat{workerGroup: "plott", good: 42, bad: 1})

	for tuple := range m.Iter() {
		m.Update(tuple.Key, func(oldVal VT) VT {
			return stat{workerGroup: oldVal.(stat).workerGroup}
		})
	}

	r, _ := m.Get("plott")
	s, ok := r.(stat)

	if !ok {
		t.Errorf("Failed to cast interface{} to stat")
	}

	if s.good != 0 {
		t.Errorf("Expected 0, got %d", s.good)
	}
}
