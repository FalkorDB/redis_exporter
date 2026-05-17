package exporter

import (
	"os"
	"strings"
	"testing"

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
		e, _ := NewRedisExporter(tst.addr, Options{Namespace: "test", IsFalkorDB: tst.isFalkorDB})

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

	e, _ := NewRedisExporter(os.Getenv("TEST_FALKORDB_URI"), Options{
		Namespace:               "test",
		IsFalkorDB:              true,
		InclFalkorDBGraphMemory: true,
	})

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

func TestEmitGraphMemoryMetrics(t *testing.T) {
	e, _ := NewRedisExporter("redis://localhost:6379", Options{
		Namespace:               "test",
		IsFalkorDB:              true,
		InclFalkorDBGraphMemory: true,
	})

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
