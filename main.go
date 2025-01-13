package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

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

// TODO(cdzombak): basic CI for lint & binaries+docker+versioning
// TODO(cdzombak): rain gauge aggregations
// TODO(cdzombak): file issue for readme
// TODO(cdzombak): file issue for increased Influx compatibility & config flexibility (eg. username, v2/v3) & validation

func main() {
	measurementName := flag.String("measurement", "weather_station", "Name of the measurement to read")
	tagsIn := flag.String("tags", "", "Comma-separated list of tag=value pairs to filter by and include in result measurements")
	windDirectionField := flag.String("wind-dir-field", "", "Name of the field to use for wind direction (in degrees); if not set, wind direction will not be aggregated")
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

	tags, err := ParseTags(*tagsIn)
	if err != nil {
		log.Fatalf("Failed to parse tags: %s", err)
	}

	if *windDirectionField != "" {
		if err := WindDirectionAgg(WindDirectionAggArgs{
			MeasurementFrom:    *measurementName,
			MeasurementTo:      *measurementName + "_agg",
			Tags:               tags,
			WindDirectionField: *windDirectionField,
			Influx:             influxClient,
			InfluxDB:           os.Getenv("INFLUX_DB"),
			InfluxRP:           os.Getenv("INFLUX_RP"),
			InfluxQueryTimeout: influxReadTimeout,
			InfluxWriteRetries: influxWriteRetries,
		}); err != nil {
			log.Fatalf("Wind direction aggregation failed: %s", err)
		}
	}

	// TODO(cdzombak): rain gauge aggregation
}

func influxHealthcheck(client influxdb.Client) error {
	_, _, err := client.Ping(influxReadTimeout)
	return err
}
