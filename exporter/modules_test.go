package exporter

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/prometheus/client_golang/prometheus"
)

func TestModulesv80(t *testing.T) {
	if os.Getenv("TEST_REDIS8_URI") == "" || os.Getenv("TEST_REDIS_URI") == "" {
		t.Skipf("TEST_REDIS8_URI or TEST_REDIS_URI aren't set - skipping")
	}

	// --- Diagnostic: directly query Redis 8 to see what INFO MODULES returns ---
	diagConn, err := redis.DialURL(os.Getenv("TEST_REDIS8_URI"))
	if err != nil {
		t.Logf("DIAG: failed to connect to Redis 8: %s", err)
	} else {
		defer diagConn.Close()

		infoModules, err := redis.String(diagConn.Do("INFO", "MODULES"))
		if err != nil {
			t.Logf("DIAG: INFO MODULES error: %s", err)
		} else {
			lines := strings.Split(infoModules, "\n")
			t.Logf("DIAG: INFO MODULES total lines: %d", len(lines))
			searchCount := 0
			for _, l := range lines {
				l = strings.TrimSpace(l)
				if strings.HasPrefix(l, "# ") || strings.HasPrefix(l, "module:") {
					t.Logf("DIAG: INFO MODULES section/module: %s", l)
				}
				if strings.HasPrefix(l, "search_") {
					searchCount++
				}
			}
			t.Logf("DIAG: INFO MODULES search_ field count: %d", searchCount)
			// Log a few specific fields we expect
			for _, l := range lines {
				l = strings.TrimSpace(l)
				if strings.HasPrefix(l, "search_used_memory_indexes:") ||
					strings.HasPrefix(l, "search_dialect_1:") ||
					strings.HasPrefix(l, "search_gc_total_cycles:") {
					t.Logf("DIAG: INFO MODULES field: %s", l)
				}
			}
		}

		infoSearch, err := redis.String(diagConn.Do("INFO", "SEARCH"))
		if err != nil {
			t.Logf("DIAG: INFO SEARCH error: %s", err)
		} else {
			lines := strings.Split(infoSearch, "\n")
			t.Logf("DIAG: INFO SEARCH total lines: %d", len(lines))
			searchCount := 0
			for _, l := range lines {
				if strings.HasPrefix(strings.TrimSpace(l), "search_") {
					searchCount++
				}
			}
			t.Logf("DIAG: INFO SEARCH search_ field count: %d", searchCount)
		}

		// Check what version of Redis we're talking to
		infoServer, err := redis.String(diagConn.Do("INFO", "SERVER"))
		if err == nil {
			for _, l := range strings.Split(infoServer, "\n") {
				l = strings.TrimSpace(l)
				if strings.HasPrefix(l, "redis_version:") {
					t.Logf("DIAG: %s", l)
				}
			}
		}
	}
	// --- End diagnostic ---

	tsts := []struct {
		addr               string
		inclModulesMetrics bool
		wantModulesMetrics bool
	}{
		{addr: os.Getenv("TEST_REDIS8_URI"), inclModulesMetrics: true, wantModulesMetrics: true},
		{addr: os.Getenv("TEST_REDIS8_URI"), inclModulesMetrics: false, wantModulesMetrics: false},
		{addr: os.Getenv("TEST_REDIS_URI"), inclModulesMetrics: true, wantModulesMetrics: false},
		{addr: os.Getenv("TEST_REDIS_URI"), inclModulesMetrics: false, wantModulesMetrics: false},
	}

	for i, tst := range tsts {
		t.Logf("DIAG: test case %d addr=%s inclModules=%v wantModules=%v", i, tst.addr, tst.inclModulesMetrics, tst.wantModulesMetrics)
		e, _ := NewRedisExporter(tst.addr, Options{Namespace: "test", InclModulesMetrics: tst.inclModulesMetrics})

		chM := make(chan prometheus.Metric)
		go func() {
			e.Collect(chM)
			close(chM)
		}()

		wantedMetrics := map[string]bool{
			"module_info":                                     false,
			"search_number_of_indexes":                        false,
			"search_used_memory_indexes_bytes":                false,
			"search_indexing_time_ms_total":                   false,
			"search_dialect_1":                                false,
			"search_dialect_2":                                false,
			"search_dialect_3":                                false,
			"search_dialect_4":                                false,
			"search_number_of_active_indexes":                 false,
			"search_number_of_active_indexes_running_queries": false,
			"search_number_of_active_indexes_indexing":        false,
			"search_total_active_write_threads":               false,
			"search_smallest_memory_index_bytes":              false,
			"search_largest_memory_index_bytes":               false,
			"search_used_memory_vector_index_bytes":           false,
			"search_global_idle_user":                         false,
			"search_global_idle_internal":                     false,
			"search_global_total_user":                        false,
			"search_global_total_internal":                    false,
			"search_gc_collected_bytes":                       false,
			"search_gc_total_docs_not_collected":              false,
			"search_gc_marked_deleted_vectors":                false,
			"search_errors_indexing_failures":                 false,
			"search_gc_cycles_total":                          false,
			"search_gc_run_ms_total":                          false,
			"search_queries_processed_total":                  false,
			"search_query_commands_total":                     false,
			"search_query_execution_time_ms_total":            false,
			"search_active_queries_total":                     false,
		}

		foundSearchMetrics := []string{}
		for m := range chM {
			desc := m.Desc().String()
			if i == 0 && (strings.Contains(desc, "search_") || strings.Contains(desc, "module_info")) {
				foundSearchMetrics = append(foundSearchMetrics, fmt.Sprintf("%s", desc))
			}
			for want := range wantedMetrics {
				if strings.Contains(desc, want) {
					wantedMetrics[want] = true
				}
			}
		}

		if i == 0 {
			t.Logf("DIAG: case %d collected %d search/module metrics", i, len(foundSearchMetrics))
			for _, d := range foundSearchMetrics {
				t.Logf("DIAG: collected: %s", d)
			}
		}

		if tst.wantModulesMetrics {
			for want, found := range wantedMetrics {
				if !found {
					t.Errorf("case %d: %s was *not* found in Redis Modules metrics but expected", i, want)
				}
			}
		} else if !tst.wantModulesMetrics {
			for want, found := range wantedMetrics {
				if found {
					t.Errorf("case %d: %s was *found* in Redis Modules metrics but *not* expected", i, want)
				}
			}
		}
	}
}

func TestModulesValkey(t *testing.T) {
	if os.Getenv("TEST_VALKEY8_BUNDLE_URI") == "" || os.Getenv("TEST_REDIS_URI") == "" {
		t.Skipf("TEST_VALKEY8_BUNDLE_URI or TEST_REDIS_URI aren't set - skipping")
	}

	tsts := []struct {
		addr               string
		inclModulesMetrics bool
		wantModulesMetrics bool
	}{
		{addr: os.Getenv("TEST_VALKEY8_BUNDLE_URI"), inclModulesMetrics: true, wantModulesMetrics: true},
		{addr: os.Getenv("TEST_VALKEY8_BUNDLE_URI"), inclModulesMetrics: false, wantModulesMetrics: false},
		{addr: os.Getenv("TEST_REDIS_URI"), inclModulesMetrics: true, wantModulesMetrics: false},
		{addr: os.Getenv("TEST_REDIS_URI"), inclModulesMetrics: false, wantModulesMetrics: false},
	}

	for _, tst := range tsts {
		e, _ := NewRedisExporter(tst.addr, Options{Namespace: "test", InclModulesMetrics: tst.inclModulesMetrics})

		chM := make(chan prometheus.Metric)
		go func() {
			e.Collect(chM)
			close(chM)
		}()

		wantedMetrics := map[string]bool{
			"module_info":                                   false,
			"search_number_of_indexes":                      false,
			"bf_bloom_total_memory_bytes":                   false,
			"bf_bloom_num_objects":                          false,
			"bf_bloom_num_filters_across_objects":           false,
			"bf_bloom_num_items_across_objects":             false,
			"bf_bloom_capacity_across_objects":              false,
			"json_total_memory_bytes":                       false,
			"json_num_documents":                            false,
			"search_used_memory_bytes":                      false,
			"search_number_of_attributes":                   false,
			"search_total_indexed_documents":                false,
			"search_query_queue_size":                       false,
			"search_writer_queue_size":                      false,
			"search_string_interning_store_size":            false,
			"search_vector_externing_hash_extern_errors":    false,
			"search_vector_externing_num_lru_entries":       false,
			"bf_bloom_defrag_hits_total":                    false,
			"bf_bloom_defrag_misses_total":                  false,
			"search_worker_pool_suspend_count":              false,
			"search_writer_resumed_count":                   false,
			"search_reader_resumed_count":                   false,
			"search_writer_suspension_expired_count":        false,
			"search_rdb_load_success_count":                 false,
			"search_rdb_load_failure_count":                 false,
			"search_rdb_save_success_count":                 false,
			"search_rdb_save_failure_count":                 false,
			"search_successful_requests_count":              false,
			"search_failure_requests_count":                 false,
			"search_hybrid_requests_count":                  false,
			"search_inline_filtering_requests_count":        false,
			"search_hnsw_add_exceptions_count":              false,
			"search_hnsw_remove_exceptions_count":           false,
			"search_hnsw_modify_exceptions_count":           false,
			"search_hnsw_search_exceptions_count":           false,
			"search_hnsw_create_exceptions_count":           false,
			"search_vector_externing_entry_count":           false,
			"search_vector_externing_generated_value_count": false,
			"search_vector_externing_lru_promote_count":     false,
			"search_vector_externing_deferred_entry_count":  false,
		}

		for m := range chM {
			for want := range wantedMetrics {
				if strings.Contains(m.Desc().String(), want) {
					wantedMetrics[want] = true
				}
			}
		}

		if tst.wantModulesMetrics {
			for want, found := range wantedMetrics {
				if !found {
					t.Errorf("%s was *not* found in Redis Modules metrics but expected", want)
				}
			}
		} else if !tst.wantModulesMetrics {
			for want, found := range wantedMetrics {
				if found {
					t.Errorf("%s was *found* in Redis Modules metrics but *not* expected", want)
				}
			}
		}
	}
}
