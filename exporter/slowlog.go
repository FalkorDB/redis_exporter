package exporter

import (
	"strconv"

	"github.com/gomodule/redigo/redis"
	"github.com/prometheus/client_golang/prometheus"
)

func (e *Exporter) extractSlowLogMetrics(ch chan<- prometheus.Metric, c redis.Conn) {
	if reply, err := redis.Int64(doRedisCmd(c, "SLOWLOG", "LEN")); err == nil {
		e.registerConstMetricGauge(ch, "slowlog_length", float64(reply))
	}

	values, err := redis.Values(doRedisCmd(c, "SLOWLOG", "GET", "1"))
	if err != nil {
		return
	}

	var slowlogLastID int64
	var lastSlowExecutionDurationSeconds float64

	if len(values) > 0 {
		if values, err = redis.Values(values[0], err); err == nil && len(values) > 0 {
			slowlogLastID = values[0].(int64)
			if len(values) > 2 {
				lastSlowExecutionDurationSeconds = float64(values[2].(int64)) / 1e6
			}
		}
	}

	e.registerConstMetricGauge(ch, "slowlog_last_id", float64(slowlogLastID))
	e.registerConstMetricGauge(ch, "last_slow_execution_duration_seconds", lastSlowExecutionDurationSeconds)
}

func (e *Exporter) extractSlowLogDetailsMetrics(ch chan<- prometheus.Metric, c redis.Conn) {
	valuesArr, err := redis.Values(doRedisCmd(c, "SLOWLOG", "GET", "10"))
	var commandDurationSeconds float64

	if err != nil {
		return
	}

	for i := 0; i < len(valuesArr); i++ {
		if values, err := redis.Values(valuesArr[i], err); err == nil && len(values) >= 4 {
			commandExecutedTimeStamp_int := values[1].(int64)
			commandExecutedTimeStamp := strconv.Itoa(int(commandExecutedTimeStamp_int))
			commandDurationSeconds = float64(values[2].(int64)) / 1e6
			commandinfo, err := redis.Values(values[3], err)

			if err != nil {
				return
			}
			commandname, err := redis.Values(commandinfo, err)
			if err != nil {
				return
			}
			command := string(commandname[0].([]uint8))
			e.registerConstMetricGauge(ch, "slowlog_history_last_ten", commandDurationSeconds, commandExecutedTimeStamp, command)
		}
	}
}
