# wx-sta-agg-influx

Aggregates wind direction and rain data from a weather station stored in InfluxDB 1.x. Reads raw sensor data from a source measurement and writes computed aggregations to a destination measurement (source name + `_agg`).

This program is designed to run periodically (e.g. via cron).

## Usage

```
wx-sta-agg-influx [flags]
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-measurement` | `weather_station` | Name of the source measurement to read |
| `-tags` | | Comma-separated `key=value` pairs to filter input data and include as tags on output points |
| `-wind-dir-field` | | Field name for wind direction (degrees). If not set, wind direction aggregation is skipped |
| `-wind-speed-field` | | Field name for wind speed. Required when `-wind-dir-field` is set |
| `-rain-field` | | Field name for rain gauge (mm). If not set, rain aggregation is skipped |
| `-env` | | Path to a `.env` file to load environment variables from |
| `-dry-run` | `false` | Print a table of points that would be written instead of writing to InfluxDB |
| `-version` | | Print version and exit |

### Environment Variables

| Variable | Description |
|----------|-------------|
| `INFLUX_SERVER` | InfluxDB server URL (e.g. `http://localhost:8086`) |
| `INFLUX_DB` | InfluxDB database name |
| `INFLUX_RP` | InfluxDB retention policy |

### Example

```sh
wx-sta-agg-influx \
  -measurement weather_station \
  -tags "station=home" \
  -wind-dir-field wind_dir \
  -wind-speed-field wind_speed \
  -rain-field rain \
  -env /etc/wx-sta-agg-influx/env
```

## Output Fields

All output is written to the measurement `<measurement>_agg` (e.g. `weather_station_agg`).

All output points include an `aggregator` tag identifying this program and its version, plus any tags specified via `-tags`.

### Wind Direction

When `-wind-dir-field` and `-wind-speed-field` are provided, the following fields are written for each interval (`5m`, `15m`, `30m`, `1h`, `3h`, `6h`):

| Field | Type | Description |
|-------|------|-------------|
| `<wind-dir-field>_mean_<interval>` | float | Weighted mean wind direction (degrees), weighted by wind speed |
| `<wind-dir-field>_stddev_<interval>` | float | Weighted standard deviation of wind direction (degrees) |
| `<wind-dir-field>_mean_intercardinal_<interval>` | string | Intercardinal direction string (e.g. `NNW`), or `VAR` if direction is too variable, or `NIL` if wind speed was zero |

An interval is only recalculated if the previous aggregation for that interval is stale.

### Rain

When `-rain-field` is provided, the following fields are written:

| Field | Type | Description |
|-------|------|-------------|
| `<rain-field>_24h` | float | Total rainfall (mm) over the past 24 hours |
| `<rain-field>_1h` | float | Total rainfall (mm) over the past 1 hour |
| `<rain-field>_rate` | float | Rain rate (mm/hr), calculated from the past 10 minutes |
| `<rain-field>_event` | float | Event rainfall total (mm); accumulates as long as rain continues, resets to zero when less than 1 mm falls in a 24-hour period |

## Installation

### Docker

Docker images are available for a variety of Linux architectures from [Docker Hub](https://hub.docker.com/r/cdzombak/wx-sta-agg-influx) and [GHCR](https://github.com/cdzombak/wx-sta-agg-influx/pkgs/container/wx-sta-agg-influx).

```shell
docker pull ghcr.io/cdzombak/wx-sta-agg-influx:latest
```

```shell
docker run --rm \
  -e INFLUX_SERVER=http://influxdb:8086 \
  -e INFLUX_DB=mydb \
  -e INFLUX_RP=autogen \
  ghcr.io/cdzombak/wx-sta-agg-influx:latest \
  -measurement weather_station \
  -tags "station=home" \
  -wind-dir-field wind_dir \
  -wind-speed-field wind_speed \
  -rain-field rain
```

### Debian via apt

Install the apt repository:

```shell
sudo apt-get install ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://dist.cdzombak.net/deb.key | sudo gpg --dearmor -o /etc/apt/keyrings/dist-cdzombak-net.gpg
sudo chmod 644 /etc/apt/keyrings/dist-cdzombak-net.gpg
sudo mkdir -p /etc/apt/sources.list.d
sudo curl -fsSL https://dist.cdzombak.net/cdzombak-oss.sources -o /etc/apt/sources.list.d/cdzombak-oss.sources
sudo chmod 644 /etc/apt/sources.list.d/cdzombak-oss.sources
sudo apt update
```

Then install the package:

```shell
sudo apt-get install wx-sta-agg-influx
```

### Homebrew (macOS)

```shell
brew install cdzombak/oss/wx-sta-agg-influx
```

### From Source

```sh
make build
```

The binary is written to `./out/wx-sta-agg-influx`.

## License

See [LICENSE](LICENSE).

## Author

Chris Dzombak
- [dzombak.com](https://dzombak.com)
- [GitHub @cdzombak](https://www.github.com/cdzombak)
