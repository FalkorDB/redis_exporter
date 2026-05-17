package exporter

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/prometheus/client_golang/prometheus"
)

func TestFalkorDB(t *testing.T) {
	if os.Getenv("TEST_FALKORDB_URI") == "" {
		t.Skipf("TEST_FALKORDB_URI not set - skipping")
	}

	tsts := []struct {
		addr                string
		isFalkorDB          bool
		wantFalkorDBMetrics bool
	}{
		{addr: os.Getenv("TEST_FALKORDB_URI"), isFalkorDB: true, wantFalkorDBMetrics: true},
		{addr: os.Getenv("TEST_FALKORDB_URI"), isFalkorDB: false, wantFalkorDBMetrics: false},
	}

	for _, tst := range tsts {
		e, err := NewRedisExporter(tst.addr, Options{Namespace: "test", IsFalkorDB: tst.isFalkorDB})
		if err != nil {
			t.Fatalf("NewRedisExporter() err: %s", err)
		}

		chM := make(chan prometheus.Metric)
		go func() {
			e.Collect(chM)
			close(chM)
		}()

		wantedMetrics := map[string]bool{
			"falkordb_total_graph_count": false,
		}

		for m := range chM {
			for want := range wantedMetrics {
				if strings.Contains(m.Desc().String(), want) {
					wantedMetrics[want] = true
				}
			}
		}

		if tst.wantFalkorDBMetrics {
			for want, found := range wantedMetrics {
				if !found {
					t.Errorf("%s was *not* found in falkordb metrics but expected", want)
				}
			}
		} else if !tst.wantFalkorDBMetrics {
			for want, found := range wantedMetrics {
				if found {
					t.Errorf("%s was *found* in falkordb metrics but *not* expected", want)
				}
			}
		}
	}
}

func TestFalkorDBGraphMemory(t *testing.T) {
	if os.Getenv("TEST_FALKORDB_URI") == "" {
		t.Skipf("TEST_FALKORDB_URI not set - skipping")
	}

	addr := os.Getenv("TEST_FALKORDB_URI")

	// Seed a minimal graph fixture to ensure deterministic metrics
	c, err := redis.DialURL(addr)
	if err != nil {
		t.Fatalf("redis.DialURL() err: %s", err)
	}
	defer c.Close()

	const testGraph = "_exporter_test_graph"
	_, err = c.Do("GRAPH.QUERY", testGraph, "CREATE (:Node{val:1})-[:EDGE]->(:Node{val:2})")
	if err != nil {
		t.Skipf("GRAPH.QUERY not supported, skipping: %s", err)
	}
	t.Cleanup(func() {
		c.Do("GRAPH.DELETE", testGraph)
	})

	e, err := NewRedisExporter(addr, Options{
		Namespace:               "test",
		IsFalkorDB:              true,
		InclFalkorDBGraphMemory: true,
	})
	if err != nil {
		t.Fatalf("NewRedisExporter() err: %s", err)
	}

	chM := make(chan prometheus.Metric)
	go func() {
		e.Collect(chM)
		close(chM)
	}()

	wantedMetrics := map[string]bool{
		"falkordb_graph_memory_total_mb":              false,
		"falkordb_graph_label_matrices_mb":            false,
		"falkordb_graph_relation_matrices_mb":         false,
		"falkordb_graph_node_block_mb":                false,
		"falkordb_graph_unlabeled_node_attributes_mb": false,
		"falkordb_graph_edge_block_mb":                false,
		"falkordb_graph_indices_mb":                   false,
	}

	for m := range chM {
		for want := range wantedMetrics {
			if strings.Contains(m.Desc().String(), want) {
				wantedMetrics[want] = true
			}
		}
	}

	for want, found := range wantedMetrics {
		if !found {
			t.Errorf("%s was *not* found in falkordb graph memory metrics but expected", want)
		}
	}
}

func TestParseFlatMap(t *testing.T) {
	// Simulate the flat array structure: ["Airport", 35, "City", 12]
	input := []interface{}{
		[]byte("Airport"), int64(35),
		[]byte("City"), int64(12),
	}

	result := parseFlatMap(input)

	if len(result) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(result))
	}
	if result["Airport"] != 35 {
		t.Errorf("expected Airport=35, got %d", result["Airport"])
	}
	if result["City"] != 12 {
		t.Errorf("expected City=12, got %d", result["City"])
	}
}

func TestParseFlatMapEmpty(t *testing.T) {
	result := parseFlatMap([]interface{}{})
	if len(result) != 0 {
		t.Errorf("expected empty map, got %d entries", len(result))
	}
}

func TestParseFlatMapNil(t *testing.T) {
	result := parseFlatMap(nil)
	if len(result) != 0 {
		t.Errorf("expected empty map for nil input, got %d entries", len(result))
	}
}

func TestParseFlatMapInvalidEntries(t *testing.T) {
	// Mix valid and invalid entries: non-string key, non-int64 value, odd length
	input := []interface{}{
		int64(999), int64(10), // key is not a string -> skip
		[]byte("Valid"), "not_an_int64", // value is not int64 -> skip
		[]byte("Good"), int64(42), // valid
		[]byte("Lonely"), // odd entry at end -> loop stops before out-of-bounds
	}

	result := parseFlatMap(input)
	if len(result) != 1 {
		t.Fatalf("expected 1 entry, got %d: %v", len(result), result)
	}
	if result["Good"] != 42 {
		t.Errorf("expected Good=42, got %d", result["Good"])
	}
}

func TestEmitGraphMemoryMetrics(t *testing.T) {
	e, err := NewRedisExporter("redis://localhost:6379", Options{
		Namespace:               "test",
		IsFalkorDB:              true,
		InclFalkorDBGraphMemory: true,
	})
	if err != nil {
		t.Fatalf("NewRedisExporter() err: %s", err)
	}

	results := []graphMemoryResult{
		{
			Graph:                  "flights",
			TotalGraphSzMB:         1086,
			LabelMatricesSzMB:      96,
			RelationMatricesSzMB:   64,
			NodeBlockSzMB:          120,
			NodeAttrsByLabel:       map[string]int64{"Airport": 35, "City": 12},
			UnlabeledNodeAttrsSzMB: 0,
			EdgeBlockSzMB:          54,
			EdgeAttrsByType:        map[string]int64{"ROUTE": 68},
			IndicesSzMB:            752,
		},
	}

	chM := make(chan prometheus.Metric, 100)
	e.emitGraphMemoryMetrics(chM, results)
	close(chM)

	wantedMetrics := map[string]bool{
		"falkordb_graph_memory_total_mb":              false,
		"falkordb_graph_label_matrices_mb":            false,
		"falkordb_graph_relation_matrices_mb":         false,
		"falkordb_graph_node_block_mb":                false,
		"falkordb_graph_node_attributes_mb":           false,
		"falkordb_graph_unlabeled_node_attributes_mb": false,
		"falkordb_graph_edge_block_mb":                false,
		"falkordb_graph_edge_attributes_mb":           false,
		"falkordb_graph_indices_mb":                   false,
	}

	for m := range chM {
		for want := range wantedMetrics {
			if strings.Contains(m.Desc().String(), want) {
				wantedMetrics[want] = true
			}
		}
	}

	for want, found := range wantedMetrics {
		if !found {
			t.Errorf("%s was *not* found in emitted metrics but expected", want)
		}
	}
}

func TestGraphListMatches(t *testing.T) {
	tests := []struct {
		name      string
		cached    []graphMemoryResult
		graphList []interface{}
		want      bool
	}{
		{
			name:      "matching single graph",
			cached:    []graphMemoryResult{{Graph: "flights"}},
			graphList: []interface{}{[]byte("flights")},
			want:      true,
		},
		{
			name:      "matching multiple graphs",
			cached:    []graphMemoryResult{{Graph: "flights"}, {Graph: "social"}},
			graphList: []interface{}{[]byte("flights"), []byte("social")},
			want:      true,
		},
		{
			name:      "different lengths - cached longer",
			cached:    []graphMemoryResult{{Graph: "flights"}, {Graph: "social"}},
			graphList: []interface{}{[]byte("flights")},
			want:      false,
		},
		{
			name:      "different lengths - graphList longer",
			cached:    []graphMemoryResult{{Graph: "flights"}},
			graphList: []interface{}{[]byte("flights"), []byte("social")},
			want:      false,
		},
		{
			name:      "same length but different names",
			cached:    []graphMemoryResult{{Graph: "flights"}},
			graphList: []interface{}{[]byte("social")},
			want:      false,
		},
		{
			name:      "empty both",
			cached:    []graphMemoryResult{},
			graphList: []interface{}{},
			want:      true,
		},
		{
			name:      "nil cached",
			cached:    nil,
			graphList: []interface{}{},
			want:      true,
		},
		{
			name:      "invalid graphList entry",
			cached:    []graphMemoryResult{{Graph: "flights"}},
			graphList: []interface{}{int64(123)},
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := graphListMatches(tt.cached, tt.graphList)
			if got != tt.want {
				t.Errorf("graphListMatches() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGraphMemoryCacheBehavior(t *testing.T) {
	e, err := NewRedisExporter("redis://localhost:6379", Options{
		Namespace:               "test",
		IsFalkorDB:              true,
		InclFalkorDBGraphMemory: true,
	})
	if err != nil {
		t.Fatalf("NewRedisExporter() err: %s", err)
	}

	// Pre-populate cache
	cachedResults := []graphMemoryResult{
		{
			Graph:            "cached_graph",
			TotalGraphSzMB:   500,
			NodeAttrsByLabel: map[string]int64{},
			EdgeAttrsByType:  map[string]int64{},
		},
	}
	e.graphMemoryCache = cachedResults
	e.graphMemoryCacheTime = time.Now()

	// Emit from cache when graphList matches
	graphList := []interface{}{[]byte("cached_graph")}
	chM := make(chan prometheus.Metric, 50)
	e.extractFalkorDBGraphMemoryMetrics(chM, nil, graphList)
	close(chM)

	found := false
	for m := range chM {
		if strings.Contains(m.Desc().String(), "falkordb_graph_memory_total_mb") {
			found = true
		}
	}
	if !found {
		t.Error("expected cached metrics to be emitted")
	}
}

func TestGraphMemoryCacheInvalidatedOnGraphListChange(t *testing.T) {
	e, err := NewRedisExporter("redis://localhost:6379", Options{
		Namespace:               "test",
		IsFalkorDB:              true,
		InclFalkorDBGraphMemory: true,
	})
	if err != nil {
		t.Fatalf("NewRedisExporter() err: %s", err)
	}

	// Pre-populate cache with one graph
	e.graphMemoryCache = []graphMemoryResult{
		{Graph: "old_graph", TotalGraphSzMB: 100, NodeAttrsByLabel: map[string]int64{}, EdgeAttrsByType: map[string]int64{}},
	}
	e.graphMemoryCacheTime = time.Now()

	// Verify cache is NOT served when graph list differs
	differentGraphList := []interface{}{[]byte("new_graph")}
	if graphListMatches(e.graphMemoryCache, differentGraphList) {
		t.Error("graphListMatches should return false for different graph lists")
	}

	// Verify cache IS served when graph list matches
	matchingGraphList := []interface{}{[]byte("old_graph")}
	if !graphListMatches(e.graphMemoryCache, matchingGraphList) {
		t.Error("graphListMatches should return true for matching graph lists")
	}

	// Test actual cache hit path
	chM := make(chan prometheus.Metric, 50)
	e.extractFalkorDBGraphMemoryMetrics(chM, nil, matchingGraphList)
	close(chM)

	found := false
	for m := range chM {
		if strings.Contains(m.Desc().String(), "falkordb_graph_memory_total_mb") {
			found = true
		}
	}
	if !found {
		t.Error("expected cached metrics to be emitted when graph list matches")
	}
}
