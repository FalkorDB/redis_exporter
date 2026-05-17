package exporter

import (
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

type graphMemoryResult struct {
	Graph                  string
	TotalGraphSzMB         int64
	LabelMatricesSzMB      int64
	RelationMatricesSzMB   int64
	NodeBlockSzMB          int64
	NodeAttrsByLabel       map[string]int64
	UnlabeledNodeAttrsSzMB int64
	EdgeBlockSzMB          int64
	EdgeAttrsByType        map[string]int64
	IndicesSzMB            int64
}

func (e *Exporter) extractFalkorDBMetrics(ch chan<- prometheus.Metric, c redis.Conn) {
	graphList, err := redis.Values(doRedisCmd(c, "GRAPH.LIST"))
	if err != nil {
		log.Errorf("extractFalkorDBMetrics() err: %s", err)
		return
	}

	graphCount := len(graphList)
	e.registerConstMetricGauge(ch, "falkordb_total_graph_count", float64(graphCount))

	if !e.options.InclFalkorDBGraphMemory {
		return
	}

	e.extractFalkorDBGraphMemoryMetrics(ch, c, graphList)
}

// extractFalkorDBGraphMemoryMetrics collects GRAPH.MEMORY USAGE for each graph.
// Note: The cache lives on the Exporter instance. When using the /scrape endpoint
// (multi-target pattern), a fresh Exporter is created per request, so the cache
// will not persist across scrapes in that mode.
func (e *Exporter) extractFalkorDBGraphMemoryMetrics(ch chan<- prometheus.Metric, c redis.Conn, graphList []interface{}) {
	ttl := e.options.FalkorDBGraphMemoryCacheTTL
	if ttl == 0 {
		ttl = 60 * time.Second
	}

	// Use cached results if still valid and graph list hasn't changed
	if e.graphMemoryCache != nil && time.Since(e.graphMemoryCacheTime) < ttl && graphListMatches(e.graphMemoryCache, graphList) {
		e.emitGraphMemoryMetrics(ch, e.graphMemoryCache)
		return
	}

	var results []graphMemoryResult

	for _, g := range graphList {
		graphName, err := redis.String(g, nil)
		if err != nil {
			log.Warnf("extractFalkorDBGraphMemoryMetrics() couldn't parse graph name: %s", err)
			continue
		}

		result, err := e.fetchGraphMemory(c, graphName)
		if err != nil {
			log.Warnf("extractFalkorDBGraphMemoryMetrics() GRAPH.MEMORY USAGE %s err: %s", graphName, err)
			continue
		}
		results = append(results, result)
	}

	// Update cache
	e.graphMemoryCache = results
	e.graphMemoryCacheTime = time.Now()

	e.emitGraphMemoryMetrics(ch, results)
}

func (e *Exporter) fetchGraphMemory(c redis.Conn, graphName string) (graphMemoryResult, error) {
	vals, err := redis.Values(doRedisCmd(c, "GRAPH.MEMORY", "USAGE", graphName))
	if err != nil {
		return graphMemoryResult{}, err
	}

	result := graphMemoryResult{
		Graph:            graphName,
		NodeAttrsByLabel: make(map[string]int64),
		EdgeAttrsByType:  make(map[string]int64),
	}

	for i := 0; i+1 < len(vals); i += 2 {
		key, err := redis.String(vals[i], nil)
		if err != nil {
			continue
		}

		switch key {
		case "total_graph_sz_mb":
			result.TotalGraphSzMB, err = redis.Int64(vals[i+1], nil)
		case "label_matrices_sz_mb":
			result.LabelMatricesSzMB, err = redis.Int64(vals[i+1], nil)
		case "relation_matrices_sz_mb":
			result.RelationMatricesSzMB, err = redis.Int64(vals[i+1], nil)
		case "amortized_node_block_sz_mb":
			result.NodeBlockSzMB, err = redis.Int64(vals[i+1], nil)
		case "amortized_node_attributes_by_label_sz_mb":
			result.NodeAttrsByLabel = parseFlatMap(vals[i+1])
		case "amortized_unlabeled_nodes_attributes_sz_mb":
			result.UnlabeledNodeAttrsSzMB, err = redis.Int64(vals[i+1], nil)
		case "amortized_edge_block_sz_mb":
			result.EdgeBlockSzMB, err = redis.Int64(vals[i+1], nil)
		case "amortized_edge_attributes_by_type_sz_mb":
			result.EdgeAttrsByType = parseFlatMap(vals[i+1])
		case "indices_sz_mb":
			result.IndicesSzMB, err = redis.Int64(vals[i+1], nil)
		}
		if err != nil {
			log.Debugf("fetchGraphMemory() couldn't parse value for key %q in graph %q: %s", key, graphName, err)
		}
	}

	return result, nil
}

// graphListMatches returns true if the cached results correspond to the same set of graphs.
func graphListMatches(cached []graphMemoryResult, graphList []interface{}) bool {
	if len(cached) != len(graphList) {
		return false
	}
	cachedNames := make(map[string]bool, len(cached))
	for _, r := range cached {
		cachedNames[r.Graph] = true
	}
	for _, g := range graphList {
		name, err := redis.String(g, nil)
		if err != nil || !cachedNames[name] {
			return false
		}
	}
	return true
}

func parseFlatMap(val interface{}) map[string]int64 {
	m := make(map[string]int64)
	items, err := redis.Values(val, nil)
	if err != nil {
		return m
	}
	for i := 0; i+1 < len(items); i += 2 {
		name, err := redis.String(items[i], nil)
		if err != nil {
			continue
		}
		v, err := redis.Int64(items[i+1], nil)
		if err != nil {
			continue
		}
		m[name] = v
	}
	return m
}

func (e *Exporter) emitGraphMemoryMetrics(ch chan<- prometheus.Metric, results []graphMemoryResult) {
	for _, r := range results {
		e.registerConstMetricGauge(ch, "falkordb_graph_memory_total_mb", float64(r.TotalGraphSzMB), r.Graph)
		e.registerConstMetricGauge(ch, "falkordb_graph_label_matrices_mb", float64(r.LabelMatricesSzMB), r.Graph)
		e.registerConstMetricGauge(ch, "falkordb_graph_relation_matrices_mb", float64(r.RelationMatricesSzMB), r.Graph)
		e.registerConstMetricGauge(ch, "falkordb_graph_node_block_mb", float64(r.NodeBlockSzMB), r.Graph)
		e.registerConstMetricGauge(ch, "falkordb_graph_unlabeled_node_attributes_mb", float64(r.UnlabeledNodeAttrsSzMB), r.Graph)
		e.registerConstMetricGauge(ch, "falkordb_graph_edge_block_mb", float64(r.EdgeBlockSzMB), r.Graph)
		e.registerConstMetricGauge(ch, "falkordb_graph_indices_mb", float64(r.IndicesSzMB), r.Graph)

		for label, val := range r.NodeAttrsByLabel {
			e.registerConstMetricGauge(ch, "falkordb_graph_node_attributes_mb", float64(val), r.Graph, label)
		}
		for relType, val := range r.EdgeAttrsByType {
			e.registerConstMetricGauge(ch, "falkordb_graph_edge_attributes_mb", float64(val), r.Graph, relType)
		}
	}
}
