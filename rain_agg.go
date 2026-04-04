package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"time"

	influxdb "github.com/influxdata/influxdb1-client/v2"
)

type RainAggArgs struct {
	MeasurementFrom string
	MeasurementTo   string
	RainField       string
	QueryTags       map[string]string
	WriteTags       map[string]string

	Influx             influxdb.Client
	InfluxDB           string
	InfluxRP           string
	InfluxQueryTimeout time.Duration
}

func RainAgg(args RainAggArgs) ([]*influxdb.Point, error) {
	// note: the given args are assumed to be valid.
	// if this were a real project or API that other people would use, I'd validate them here.

	tagsWhere := PartialWhereClauseForTags(args.QueryTags)

	// 24h
	// gather the data we'll need:
	q := fmt.Sprintf("SELECT time, %s FROM %s WHERE time >= now()-24h %s ORDER BY time ASC",
		args.RainField, args.MeasurementFrom, tagsWhere)
	log.Printf("[DEBUG] query: %s", q)
	r, err := args.Influx.Query(influxdb.Query{
		Command:         q,
		Database:        args.InfluxDB,
		RetentionPolicy: args.InfluxRP,
	})
	if err != nil {
		return nil, fmt.Errorf("InfluxDB query failed: %w", err)
	}
	if r.Err != "" {
		return nil, fmt.Errorf("InfluxDB query failed: %s", r.Err)
	}
	if len(r.Results) == 0 || len(r.Results[0].Series) == 0 {
		log.Printf("no data to aggregate")
		return nil, nil
	}
	if len(r.Results) > 1 {
		return nil, fmt.Errorf("expected 1 result, got %d", len(r.Results))
	}
	if len(r.Results[0].Series) > 1 {
		return nil, fmt.Errorf("expected 1 series, got %d", len(r.Results[0].Series))
	}
	if r.Results[0].Series[0].Columns[0] != "time" {
		return nil, fmt.Errorf("expected first column to be 'time', got '%s'", r.Results[0].Series[0].Columns[0])
	}
	if r.Results[0].Series[0].Columns[1] != args.RainField {
		return nil, fmt.Errorf("expected second column to be '%s', got '%s'", args.RainField, r.Results[0].Series[0].Columns[1])
	}

	rainTotal := 0.0
	prevDataPoint := math.NaN()
	lastDataPointTime := time.Time{}
	for _, sourceDataPoint := range r.Results[0].Series[0].Values {
		if sourceDataPoint[1] == nil {
			continue
		}
		lastDataPointTime, err = time.Parse(time.RFC3339, sourceDataPoint[0].(string))
		if err != nil {
			return nil, fmt.Errorf("failed to parse timestamp: %w", err)
		}
		rainSensor, err := sourceDataPoint[1].(json.Number).Float64()
		if err != nil {
			return nil, fmt.Errorf("failed to parse rain sensor value: %w", err)
		}
		if !math.IsNaN(prevDataPoint) {
			// skip rollover:
			if rainSensor < prevDataPoint {
				prevDataPoint = rainSensor
				continue
			}
			rainTotal += rainSensor - prevDataPoint
		}
		prevDataPoint = rainSensor
	}

	p, err := influxdb.NewPoint(
		args.MeasurementTo,
		args.WriteTags,
		map[string]any{
			args.RainField + "_24h": rainTotal,
		},
		lastDataPointTime,
	)

	return []*influxdb.Point{p}, err
}
