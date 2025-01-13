package main

import (
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"time"

	"github.com/avast/retry-go"
	"github.com/cdzombak/libwx"
	influxdb "github.com/influxdata/influxdb1-client/v2"
)

type WindDirectionAggArgs struct {
	MeasurementFrom    string
	MeasurementTo      string
	WindDirectionField string
	WindSpeedField     string
	Tags               map[string]string

	Influx             influxdb.Client
	InfluxDB           string
	InfluxRP           string
	InfluxQueryTimeout time.Duration
	InfluxWriteRetries uint
}

const (
	wdInterval6h  = "6h"
	wdInterval3h  = "3h"
	wdInterval1h  = "1h"
	wdInterval30m = "30m"
	wdInterval15m = "15m"
	wdInterval5m  = "5m"
)

func allWindDirectionIntervals() []string {
	return []string{
		wdInterval6h,
		wdInterval3h,
		wdInterval1h,
		wdInterval30m,
		wdInterval15m,
		wdInterval5m,
	}
}

func windDirIntervalToDuration(interval string) time.Duration {
	switch interval {
	case wdInterval6h:
		return 6 * time.Hour
	case wdInterval3h:
		return 3 * time.Hour
	case wdInterval1h:
		return time.Hour
	case wdInterval30m:
		return 30 * time.Minute
	case wdInterval15m:
		return 15 * time.Minute
	case wdInterval5m:
		return 5 * time.Minute
	default:
		panic(fmt.Sprintf("unknown interval: %s", interval))
	}
}

func maxTimeBetweenAggsForWindDirInterval(interval string) time.Duration {
	switch interval {
	case wdInterval6h:
		return 1 * time.Hour
	case wdInterval3h:
		return 40 * time.Minute
	case wdInterval1h:
		return 20 * time.Minute
	case wdInterval30m:
		return 10 * time.Minute
	case wdInterval15m:
		return 5 * time.Minute
	case wdInterval5m:
		return 2*time.Minute + 30*time.Second
	default:
		panic(fmt.Sprintf("unknown interval: %s", interval))
	}
}

func stdDevThresholdsForWindDirIntervalCardinalResult(interval string) (float64, float64) {
	// returns: max (weighted) SD for secondary intercardinal, max SD for primary intercardinal (otherwise VAR)
	switch interval {
	case wdInterval6h:
		return 30, 38
	case wdInterval3h:
		return 35, 40
	case wdInterval1h:
		return 38, 43
	case wdInterval30m:
		return 42, 45
	case wdInterval15m:
		return 43.5, 46
	case wdInterval5m:
		return 45, 46.5
	default:
		panic(fmt.Sprintf("unknown interval: %s", interval))
	}
}

func wdMeanResultFieldName(args WindDirectionAggArgs, interval string) string {
	return args.WindDirectionField + "_mean_" + interval
}

func wdStdDevResultFieldName(args WindDirectionAggArgs, interval string) string {
	return args.WindDirectionField + "_stddev_" + interval
}

func wdMeanIntercardinalResultFieldName(args WindDirectionAggArgs, interval string) string {
	return args.WindDirectionField + "_mean_intercardinal_" + interval
}

type wdDataPoint struct {
	dir libwx.Degree
	spd float64
}

func dirSeries(data []wdDataPoint) []libwx.Degree {
	retv := make([]libwx.Degree, len(data))
	for i, dp := range data {
		retv[i] = dp.dir
	}
	return retv
}

func spdSeries(data []wdDataPoint) []float64 {
	retv := make([]float64, len(data))
	for i, dp := range data {
		retv[i] = dp.spd
	}
	return retv
}

func WindDirectionAgg(args WindDirectionAggArgs) error {
	// note: the given args are assumed to be valid.
	// if this were a real project or API that other people would use, I'd validate them here.

	tagsWhere := PartialWhereClauseForTags(args.Tags)

	// first, figure out which intervals we need to calculate.
	var intervalsTodo []string
	for _, interval := range allWindDirectionIntervals() {
		resultFieldName := wdMeanResultFieldName(args, interval)
		q := fmt.Sprintf("SELECT time, %s FROM %s WHERE time >= now()-%s %s ORDER BY time DESC LIMIT 1", resultFieldName, args.MeasurementTo, interval, tagsWhere)
		log.Printf("[DEBUG] query: %s", q)
		r, err := args.Influx.Query(influxdb.Query{
			Command:         q,
			Database:        args.InfluxDB,
			RetentionPolicy: args.InfluxRP,
		})
		if err != nil {
			return fmt.Errorf("InfluxDB query failed: %w", err)
		}
		if r.Err != "" {
			return fmt.Errorf("InfluxDB query failed: %s", r.Err)
		}

		if len(r.Results) == 0 || len(r.Results[0].Series) == 0 {
			intervalsTodo = append(intervalsTodo, interval)
			continue
		}

		if len(r.Results) > 1 {
			return fmt.Errorf("expected 1 result, got %d", len(r.Results))
		}
		if len(r.Results[0].Series) > 1 {
			return fmt.Errorf("expected 1 series, got %d", len(r.Results[0].Series))
		}
		if r.Results[0].Series[0].Columns[0] != "time" {
			return fmt.Errorf("expected first column to be 'time', got '%s'", r.Results[0].Series[0].Columns[0])
		}

		t, err := time.Parse(time.RFC3339, r.Results[0].Series[0].Values[0][0].(string))
		if err != nil {
			return fmt.Errorf("failed to parse time: %w", err)
		}
		if time.Since(t.Add(windDirIntervalToDuration(interval)/2)) > maxTimeBetweenAggsForWindDirInterval(interval) {
			intervalsTodo = append(intervalsTodo, interval)
		}
	}

	if len(intervalsTodo) == 0 {
		log.Printf("no intervals to calculate")
		return nil
	}

	now := time.Now()

	// gather the data we'll need:
	q := fmt.Sprintf("SELECT time, %s, %s FROM %s WHERE time >= now()-%s %s ORDER BY time ASC",
		args.WindDirectionField, args.WindSpeedField, args.MeasurementFrom, intervalsTodo[0], tagsWhere)
	log.Printf("[DEBUG] query: %s", q)
	r, err := args.Influx.Query(influxdb.Query{
		Command:         q,
		Database:        args.InfluxDB,
		RetentionPolicy: args.InfluxRP,
	})
	if err != nil {
		return fmt.Errorf("InfluxDB query failed: %w", err)
	}
	if r.Err != "" {
		return fmt.Errorf("InfluxDB query failed: %s", r.Err)
	}
	if len(r.Results) == 0 || len(r.Results[0].Series) == 0 {
		log.Printf("no data to aggregate")
		return nil
	}

	if len(r.Results) > 1 {
		return fmt.Errorf("expected 1 result, got %d", len(r.Results))
	}
	if len(r.Results[0].Series) > 1 {
		return fmt.Errorf("expected 1 series, got %d", len(r.Results[0].Series))
	}
	if r.Results[0].Series[0].Columns[0] != "time" {
		return fmt.Errorf("expected first column to be 'time', got '%s'", r.Results[0].Series[0].Columns[0])
	}
	if r.Results[0].Series[0].Columns[1] != args.WindDirectionField {
		return fmt.Errorf("expected second column to be '%s', got '%s'", args.WindDirectionField, r.Results[0].Series[0].Columns[1])
	}
	if r.Results[0].Series[0].Columns[2] != args.WindSpeedField {
		return fmt.Errorf("expected thirs column to be '%s', got '%s'", args.WindSpeedField, r.Results[0].Series[0].Columns[2])
	}

	// aggregate data by interval:
	// create aggregate & output data structures:
	intervalData := make(map[string][]wdDataPoint)
	for _, interval := range intervalsTodo {
		intervalData[interval] = []wdDataPoint{}
	}
	for _, sourceDataPoint := range r.Results[0].Series[0].Values {
		// this parsing could be cleaned up and made a lot more robust.
		dir, err := sourceDataPoint[1].(json.Number).Float64()
		if err != nil {
			return fmt.Errorf("failed to parse wind direction: %w", err)
		}
		spd, err := sourceDataPoint[2].(json.Number).Float64()
		if err != nil {
			return fmt.Errorf("failed to parse wind speed: %w", err)
		}
		dp := wdDataPoint{
			dir: libwx.Degree(dir).Clamped(),
			spd: spd,
		}
		t, err := time.Parse(time.RFC3339, sourceDataPoint[0].(string))
		if err != nil {
			return fmt.Errorf("failed to parse time: %w", err)
		}
		for _, interval := range intervalsTodo {
			if now.Sub(t) <= windDirIntervalToDuration(interval) {
				intervalData[interval] = append(intervalData[interval], dp)
			}
		}
	}

	bp, err := influxdb.NewBatchPoints(influxdb.BatchPointsConfig{
		Database:        args.InfluxDB,
		RetentionPolicy: args.InfluxRP,
	})
	if err != nil {
		return fmt.Errorf("failed to create InfluxDB batch: %w", err)
	}
	tags := map[string]string{
		"aggregator": fmt.Sprintf("%s/%s", ProductName, Version),
	}
	maps.Copy(tags, args.Tags)

	for _, interval := range intervalsTodo {
		if len(intervalData[interval]) == 0 {
			continue
		}
		fields := make(map[string]interface{})
		mean, err := libwx.WeightedAvgDirectionDeg(dirSeries(intervalData[interval]), spdSeries(intervalData[interval]))
		if err != nil {
			return fmt.Errorf("failed to calculate weighted average wind direction: %w", err)
		}
		stdDev, err := libwx.WeightedStdDevDirectionDeg(dirSeries(intervalData[interval]), spdSeries(intervalData[interval]))
		if err != nil {
			return fmt.Errorf("failed to calculate weighted stddev of wind direction: %w", err)
		}
		maxSecInt, maxInt := stdDevThresholdsForWindDirIntervalCardinalResult(interval)
		card := ""
		if stdDev.Unwrap() > maxInt {
			card = "VAR"
		} else if stdDev.Unwrap() > maxSecInt {
			card = libwx.DirectionStr(mean, libwx.DirectionStrPrecision1)
		} else {
			card = libwx.DirectionStr(mean, libwx.DirectionStrPrecision2)
		}
		fields[wdMeanResultFieldName(args, interval)] = mean.Unwrap()
		fields[wdStdDevResultFieldName(args, interval)] = stdDev.Unwrap()
		fields[wdMeanIntercardinalResultFieldName(args, interval)] = card

		point, err := influxdb.NewPoint(
			args.MeasurementTo,
			tags,
			fields,
			now.Add(-1*windDirIntervalToDuration(interval)/2),
		)
		if err != nil {
			return fmt.Errorf("failed to create InfluxDB point: %w", err)
		}
		bp.AddPoint(point)
	}

	if err := retry.Do(
		func() error {
			return args.Influx.Write(bp)
		},
		retry.Attempts(args.InfluxWriteRetries),
	); err != nil {
		log.Printf("failed to write to Influx: %s", err.Error())
	}

	return nil
}
