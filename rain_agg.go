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

	rainEventResetThreshold = 1.0 // mm in 24h to keep an event active
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

func rainEventFieldName(args RainAggArgs) string {
	return args.RainField + "_event"
}

type rainDataPoint struct {
	t    time.Time
	rain float64
}

// accumRain calculates total rainfall from a series of gauge readings,
// skipping rollovers (where the gauge value decreases).
func accumRain(data []rainDataPoint) float64 {
	total := 0.0
	prev := math.NaN()
	for _, dp := range data {
		if !math.IsNaN(prev) {
			if dp.rain < prev {
				prev = dp.rain
				continue
			}
			total += dp.rain - prev
		}
		prev = dp.rain
	}
	return total
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

	// rain totals per interval:
	var rain24h float64
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

		rainTotal := accumRain(intervalData)
		if interval == rainInterval24h {
			rain24h = rainTotal
		}

		p, err := influxdb.NewPoint(
			args.MeasurementTo,
			args.WriteTags,
			map[string]any{
				rainResultFieldName(args, interval): rainTotal,
			},
			intervalData[len(intervalData)-1].t,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create InfluxDB point: %w", err)
		}
		retv = append(retv, p)
	}

	// rain rate (rain over past 10 minutes, extrapolated to per-hour):
	var rateData []rainDataPoint
	for _, dp := range allData {
		if latestTime.Sub(dp.t) <= 10*time.Minute {
			rateData = append(rateData, dp)
		}
	}
	if len(rateData) > 0 {
		p, err := influxdb.NewPoint(
			args.MeasurementTo,
			args.WriteTags,
			map[string]any{
				args.RainField + "_rate": accumRain(rateData) * 6,
			},
			latestTime,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create InfluxDB point: %w", err)
		}
		retv = append(retv, p)
	}

	// event rainfall (continuous rain; resets when 24h total < 1mm):
	eventTotal, err := rainEventAgg(args, tagsWhere, rain24h)
	if err != nil {
		return nil, fmt.Errorf("rain event aggregation failed: %w", err)
	}
	p, err := influxdb.NewPoint(
		args.MeasurementTo,
		args.WriteTags,
		map[string]any{
			rainEventFieldName(args): eventTotal,
		},
		latestTime,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create InfluxDB point: %w", err)
	}
	retv = append(retv, p)

	return retv, nil
}

func rainEventAgg(args RainAggArgs, tagsWhere string, rain24h float64) (float64, error) {
	if rain24h < rainEventResetThreshold {
		return 0, nil
	}

	// read the previous event total from the agg measurement:
	eventField := rainEventFieldName(args)
	q := fmt.Sprintf("SELECT time, %s FROM %s WHERE %s != '' %s ORDER BY time DESC LIMIT 1",
		eventField, args.MeasurementTo, eventField, tagsWhere)
	log.Printf("[DEBUG] query: %s", q)
	r, err := args.Influx.Query(influxdb.Query{
		Command:         q,
		Database:        args.InfluxDB,
		RetentionPolicy: args.InfluxRP,
	})
	if err != nil {
		return 0, fmt.Errorf("InfluxDB query failed: %w", err)
	}
	if r.Err != "" {
		return 0, fmt.Errorf("InfluxDB query failed: %s", r.Err)
	}

	// if no previous event total exists, fall back to the 24h total:
	if len(r.Results) == 0 || len(r.Results[0].Series) == 0 {
		return rain24h, nil
	}

	prevEventTotal := 0.0
	prevEventTime := time.Time{}

	if r.Results[0].Series[0].Values[0][1] != nil {
		prevEventTotal, err = r.Results[0].Series[0].Values[0][1].(json.Number).Float64()
		if err != nil {
			return 0, fmt.Errorf("failed to parse previous event total: %w", err)
		}
	}
	prevEventTime, err = time.Parse(time.RFC3339, r.Results[0].Series[0].Values[0][0].(string))
	if err != nil {
		return 0, fmt.Errorf("failed to parse previous event time: %w", err)
	}

	// if the previous event value was a reset (0), use 24h total as the new event total:
	if prevEventTotal == 0 {
		return rain24h, nil
	}

	// query raw rain data since the previous event agg to calculate new rain:
	q = fmt.Sprintf("SELECT time, %s FROM %s WHERE time > '%s' %s ORDER BY time ASC",
		args.RainField, args.MeasurementFrom, prevEventTime.Format(time.RFC3339), tagsWhere)
	log.Printf("[DEBUG] query: %s", q)
	r, err = args.Influx.Query(influxdb.Query{
		Command:         q,
		Database:        args.InfluxDB,
		RetentionPolicy: args.InfluxRP,
	})
	if err != nil {
		return 0, fmt.Errorf("InfluxDB query failed: %w", err)
	}
	if r.Err != "" {
		return 0, fmt.Errorf("InfluxDB query failed: %s", r.Err)
	}
	if len(r.Results) == 0 || len(r.Results[0].Series) == 0 {
		return prevEventTotal, nil
	}

	var newData []rainDataPoint
	for _, v := range r.Results[0].Series[0].Values {
		if v[1] == nil {
			continue
		}
		rainVal, err := v[1].(json.Number).Float64()
		if err != nil {
			return 0, fmt.Errorf("failed to parse rain sensor value: %w", err)
		}
		newData = append(newData, rainDataPoint{rain: rainVal})
	}

	return prevEventTotal + accumRain(newData), nil
}
