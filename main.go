package main

import (
	"flag"
	"fmt"
	"log"
	"maps"
	"os"
	"time"

	"github.com/avast/retry-go"
	ec "github.com/cdzombak/exitcode_go"
	influxdb "github.com/influxdata/influxdb1-client/v2"
	"github.com/joho/godotenv"
)

const (
	influxReadTimeout  = 30 * time.Second
	influxWriteTimeout = 5 * time.Second
	influxWriteRetries = 2

	ProductName = "wx-station-aggregator-influx"
)

var Version = "<dev>"

func main() {
	measurementName := flag.String("measurement", "weather_station", "Name of the measurement to read")
	tagsIn := flag.String("tags", "", "Comma-separated list of tag=value pairs to filter by and include in result measurements")
	windDirectionField := flag.String("wind-dir-field", "", "Name of the field to use for wind direction (in degrees); if not set, wind direction will not be aggregated")
	windSpeedField := flag.String("wind-speed-field", "", "Name of the field to use for wind speed; required iff wind-dir-field is given")
	// rainGaugeField := flag.String("rain-field", "", "Name of the field to use for rain gauge (in mm); if not set, rain gauge will not be aggregated")
	envFileName := flag.String("env", "", "Path to .env file to load environment variables from")
	printVersion := flag.Bool("version", false, "Print version and exit")
	flag.Parse()

	if *printVersion {
		fmt.Printf("%s version %s\n", ProductName, Version)
		os.Exit(ec.Success)
	}

	if *envFileName != "" {
		if err := godotenv.Load(*envFileName); err != nil {
			log.Fatalf("Failed to load '%s': %v", *envFileName, err)
		}
	}

	influxClient, err := influxdb.NewHTTPClient(influxdb.HTTPConfig{
		Addr:    os.Getenv("INFLUX_SERVER"),
		Timeout: influxWriteTimeout,
	})
	if err != nil {
		log.Fatalf("Failed to create InfluxDB client: %s", err)
	}
	if err := influxHealthcheck(influxClient); err != nil {
		log.Fatalf("InfluxDB ping failed: %s", err)
	}
	defer influxClient.Close()

	qTags, err := ParseTags(*tagsIn)
	if err != nil {
		log.Fatalf("Failed to parse tags: %s", err)
	}

	wTags := map[string]string{
		"aggregator": fmt.Sprintf("%s/%s", ProductName, Version),
	}
	maps.Copy(wTags, qTags)

	if *windDirectionField != "" && *windSpeedField == "" {
		log.Fatalln("wind-speed-field is required when wind-dir-field is set")
	}

	var points []*influxdb.Point

	if *windDirectionField != "" {
		wdPoints, err := WindDirectionAgg(WindDirectionAggArgs{
			MeasurementFrom:    *measurementName,
			MeasurementTo:      *measurementName + "_agg",
			QueryTags:          qTags,
			WriteTags:          wTags,
			WindDirectionField: *windDirectionField,
			WindSpeedField:     *windSpeedField,
			Influx:             influxClient,
			InfluxDB:           os.Getenv("INFLUX_DB"),
			InfluxRP:           os.Getenv("INFLUX_RP"),
			InfluxQueryTimeout: influxReadTimeout,
		})
		if err != nil {
			log.Fatalf("Wind direction aggregation failed: %s", err)
		}
		points = append(points, wdPoints...)
	}

	// TODO(cdzombak): rain gauge aggregation goes here, if rainGaugeField is set
	//                 https://github.com/cdzombak/wx-sta-agg-influx/issues/3

	if len(points) == 0 {
		log.Printf("no data to write")
		return
	}

	bp, err := influxdb.NewBatchPoints(influxdb.BatchPointsConfig{
		Database:        os.Getenv("INFLUX_DB"),
		RetentionPolicy: os.Getenv("INFLUX_RP"),
	})
	if err != nil {
		log.Fatalf("failed to create InfluxDB batch: %s", err)
	}

	bp.AddPoints(points)

	if err := retry.Do(
		func() error {
			return influxClient.Write(bp)
		},
		retry.Attempts(influxWriteRetries),
	); err != nil {
		log.Printf("failed to write to Influx: %s", err.Error())
	}
}

func influxHealthcheck(client influxdb.Client) error {
	_, _, err := client.Ping(influxReadTimeout)
	return err
}
