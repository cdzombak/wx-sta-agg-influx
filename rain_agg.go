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

const (
	rainInterval24h = "24h"
	rainInterval1h  = "1h"
)

func allRainIntervals() []string {
	return []string{rainInterval24h, rainInterval1h}
}

func rainIntervalToDuration(interval string) time.Duration {
	switch interval {
	case rainInterval24h:
		return 24 * time.Hour
	case rainInterval1h:
		return time.Hour
	default:
		panic(fmt.Sprintf("unknown rain interval: %s", interval))
	}
}

func rainResultFieldName(args RainAggArgs, interval string) string {
	return args.RainField + "_" + interval
}

type rainDataPoint struct {
	t    time.Time
	rain float64
}

func RainAgg(args RainAggArgs) ([]*influxdb.Point, error) {
	// note: the given args are assumed to be valid.
	// if this were a real project or API that other people would use, I'd validate them here.

	tagsWhere := PartialWhereClauseForTags(args.QueryTags)

	// query for the longest interval; shorter intervals will filter from this data.
	q := fmt.Sprintf("SELECT time, %s FROM %s WHERE time >= now()-%s %s ORDER BY time ASC",
		args.RainField, args.MeasurementFrom, rainInterval24h, tagsWhere)
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
		log.Printf("no rain data to aggregate")
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

	// parse all data points from the query result:
	var allData []rainDataPoint
	for _, sourceDataPoint := range r.Results[0].Series[0].Values {
		if sourceDataPoint[1] == nil {
			continue
		}
		t, err := time.Parse(time.RFC3339, sourceDataPoint[0].(string))
		if err != nil {
			return nil, fmt.Errorf("failed to parse timestamp: %w", err)
		}
		rainSensor, err := sourceDataPoint[1].(json.Number).Float64()
		if err != nil {
			return nil, fmt.Errorf("failed to parse rain sensor value: %w", err)
		}
		allData = append(allData, rainDataPoint{t: t, rain: rainSensor})
	}

	if len(allData) == 0 {
		log.Printf("no rain data to aggregate")
		return nil, nil
	}

	latestTime := allData[len(allData)-1].t
	var retv []*influxdb.Point

	for _, interval := range allRainIntervals() {
		dur := rainIntervalToDuration(interval)

		// filter data points for this interval:
		var intervalData []rainDataPoint
		for _, dp := range allData {
			if latestTime.Sub(dp.t) <= dur {
				intervalData = append(intervalData, dp)
			}
		}

		if len(intervalData) == 0 {
			continue
		}

		// sum rain, skipping rollovers:
		rainTotal := 0.0
		prevDataPoint := math.NaN()
		lastDataPointTime := intervalData[len(intervalData)-1].t
		for _, dp := range intervalData {
			if !math.IsNaN(prevDataPoint) {
				if dp.rain < prevDataPoint {
					prevDataPoint = dp.rain
					continue
				}
				rainTotal += dp.rain - prevDataPoint
			}
			prevDataPoint = dp.rain
		}

		p, err := influxdb.NewPoint(
			args.MeasurementTo,
			args.WriteTags,
			map[string]any{
				rainResultFieldName(args, interval): rainTotal,
			},
			lastDataPointTime,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create InfluxDB point: %w", err)
		}
		retv = append(retv, p)
	}

	// calculate rain rate (rain over past 10 minutes, extrapolated to per-hour):
	var rateData []rainDataPoint
	for _, dp := range allData {
		if latestTime.Sub(dp.t) <= 10*time.Minute {
			rateData = append(rateData, dp)
		}
	}
	if len(rateData) > 0 {
		rateTotal := 0.0
		prevDP := math.NaN()
		for _, dp := range rateData {
			if !math.IsNaN(prevDP) {
				if dp.rain < prevDP {
					prevDP = dp.rain
					continue
				}
				rateTotal += dp.rain - prevDP
			}
			prevDP = dp.rain
		}

		p, err := influxdb.NewPoint(
			args.MeasurementTo,
			args.WriteTags,
			map[string]any{
				args.RainField + "_rate": rateTotal * 6,
			},
			latestTime,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create InfluxDB point: %w", err)
		}
		retv = append(retv, p)
	}

	return retv, nil
}
