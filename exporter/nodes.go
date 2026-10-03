package exporter

import (
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/gomodule/redigo/redis"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

var reNodeAddress = regexp.MustCompile(`^(?P<ip>.+):(?P<port>\d+)@(?P<cport>\d+)(?:,(?P<hostname>.+))?`)

func (e *Exporter) getClusterNodes(c redis.Conn) ([]string, error) {
	output, err := redis.String(doRedisCmd(c, "CLUSTER", "NODES"))
	if err != nil {
		log.Errorf("Error getting cluster nodes: %s", err)
		return nil, err
	}

	lines := strings.Split(output, "\n")
	nodes := []string{}

	for _, line := range lines {
		if node, ok := parseClusterNodeString(line, e.options.ClusterDiscoverHostnames); ok {
			nodes = append(nodes, node)
		}
	}

	return nodes, nil
}

/*
<id> <ip:port@cport[,hostname]> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot> <slot> ... <slot>
eaf69c70d876558a948ba62af0884a37d42c9627 127.0.0.1:7002@17002 master - 0 1742836359057 3 connected 10923-16383
*/
func parseClusterNodeString(node string, resolveHostname bool) (string, bool) {
	log.Debugf("parseClusterNodeString node: [%s]", node)

	fields := strings.Fields(node)
	if len(fields) < 2 {
		log.Debugf("Invalid field count for node: %s", node)
		return "", false
	}

	address := reNodeAddress.FindStringSubmatch(fields[1])
	if len(address) < 3 {
		log.Debugf("Invalid format for node address, got: %s", fields[1])
		return "", false
	}

	// address[1] = ip, address[2] = port, address[4] = hostname (may be empty)
	if resolveHostname && len(address) >= 5 && address[4] != "" {
		return address[4] + ":" + address[2], true
	}
	return address[1] + ":" + address[2], true
}

// extractClusterNodeShardMetrics exports the shard identity of the scraped node,
// discovered dynamically from CLUSTER NODES (never from static configuration).
// The shard is identified by the slot ranges owned by the shard's master, so
// masters and their replicas share the same shard_slots label value.
func (e *Exporter) extractClusterNodeShardMetrics(ch chan<- prometheus.Metric, c redis.Conn) {
	output, err := redis.String(doRedisCmd(c, "CLUSTER", "NODES"))
	if err != nil {
		log.Errorf("Error getting cluster nodes: %s", err)
		return
	}

	if shardSlots, ok := parseShardSlotsFromClusterNodes(output); ok {
		e.registerConstMetricGauge(ch, "cluster_node_shard", 1, shardSlots)
	} else {
		log.Debugf("could not determine shard slots from CLUSTER NODES output")
	}
}

/*
parseShardSlotsFromClusterNodes finds the "myself" node in CLUSTER NODES output and
returns the normalized slot ranges of the shard it belongs to. For a replica the
slots are resolved via its master's line. Slot ranges are sorted numerically by
start slot and joined with "," so that all members of a shard produce an identical
value. Importing/migrating slot entries (e.g. "[5461->-<id>]") are ignored.

line format:
<id> <ip:port@cport[,hostname]> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot> <slot> ... <slot>
*/
func parseShardSlotsFromClusterNodes(clusterNodes string) (string, bool) {
	nodeSlots := map[string][]string{} // node id -> slot ranges
	myselfID := ""
	myselfMasterID := ""

	for _, line := range strings.Split(clusterNodes, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 8 {
			continue
		}
		id, flags, masterID := fields[0], fields[2], fields[3]

		var slots []string
		for _, slot := range fields[8:] {
			if strings.HasPrefix(slot, "[") {
				// skip importing/migrating slot entries
				continue
			}
			slots = append(slots, slot)
		}
		nodeSlots[id] = slots

		if strings.Contains(flags, "myself") {
			myselfID = id
			myselfMasterID = masterID
		}
	}

	if myselfID == "" {
		return "", false
	}

	shardMasterID := myselfID
	if myselfMasterID != "-" {
		// myself is a replica; the shard's slots live on its master
		shardMasterID = myselfMasterID
	}

	slots := nodeSlots[shardMasterID]
	if len(slots) == 0 {
		return "", false
	}

	sort.Slice(slots, func(i, j int) bool {
		return slotRangeStart(slots[i]) < slotRangeStart(slots[j])
	})
	return strings.Join(slots, ","), true
}

func slotRangeStart(s string) int {
	start, _, _ := strings.Cut(s, "-")
	n, err := strconv.Atoi(start)
	if err != nil {
		return -1
	}
	return n
}
