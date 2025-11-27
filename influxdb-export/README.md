# InfluxDB Export Module

Data pipeline for exporting metrics and sensor data from Kafka and Prometheus to InfluxDB for long-term storage and analysis.

## Overview

This module provides containerized exporters that continuously stream data to InfluxDB:
- **Kafka → InfluxDB**: Consumes SWaT sensor data from Kafka topics
- **Prometheus → InfluxDB**: Scrapes Prometheus metrics for historical storage

Both exporters run as Docker containers with automatic restarts and health monitoring.

## Files

### Docker Configuration

#### `Dockerfile`
Multi-purpose container image for both exporters.

**Features:**
- Python 3.11 slim base
- Pre-installed dependencies: `kafka-python`, `influxdb-client`, `pandas`, `prometheus-api-client`
- Built-in debugging tools (`procps`, `curl`, `bash`)
- Line ending normalization (`dos2unix`)
- Health check script for connectivity testing

#### `entrypoint.sh`
Eentrypoint that routes to the appropriate Python script based on `PYTHON_SCRIPT` environment variable.

**Routes:**
- `kafka_to_influx.py` → Kafka consumer with full parameters
- `prometheus_to_influx.py` → Prometheus scraper with configurable interval

**Parameters passed:**
- Kafka: `--bootstrap`, `--topic`, `--influx-url`, `--influx-org`, `--influx-bucket`, `--token`
- Prometheus: `--prometheus-url`, `--influx-url`, `--influx-org`, `--influx-bucket`, `--token`, `--interval`

### Exporters

#### `kafka_to_influx.py`
Kafka consumer that reads SWaT sensor data and writes to InfluxDB in batches.

**Features:**
- Consumes from `ics-sensor-data` topic
- Dynamic field mapping (all numeric values become InfluxDB fields)
- Timestamp parsing from CSV format: `DD/MM/YYYY HH:MM:SS AM/PM`
- Batched writes for performance (default: 10 points per batch)
- Attack status tagging (`Normal` vs `Attack`)
- Auto-commit for offset management

**Command-line Arguments:**
```bash
--bootstrap <kafka_server>      # Kafka bootstrap server (default: localhost:9092)
--topic <topic_name>            # Kafka topic (default: ics-sensor-data)
--influx-url <url>              # InfluxDB URL (default: http://localhost:8086)
--influx-org <org>              # InfluxDB organization (default: ucs)
--influx-bucket <bucket>        # Target bucket (default: swat_db)
--token <token>                 # InfluxDB authentication token (required)
--group <group_id>              # Consumer group ID
--batch-size <n>                # Points per batch (default: 10)
--max-messages <n>              # Max messages to process (default: unlimited)
```

**Data Processing:**
1. Deserializes JSON from Kafka
2. Extracts `Timestamp` and `Normal_Attack` fields
3. Converts all other fields to float (sensor readings)
4. Tags point with `status` (Normal/Attack)
5. Batches points and writes to InfluxDB

**Error Handling:**
- Skips non-numeric values with warning
- Handles timestamp parsing failures gracefully
- Catches and logs InfluxDB write errors
- Writes remaining batch on shutdown

**Example:**
```bash
python kafka_to_influx.py \
  --bootstrap kafka:9092 \
  --topic ics-sensor-data \
  --influx-url http://influxdb:8086 \
  --influx-org ucs \
  --influx-bucket swat_db \
  --token admintoken123 \
  --batch-size 50
```

#### `prometheus_to_influx.py`
Prometheus scraper that periodically queries metrics and writes to InfluxDB.

**Features:**
- Configurable scrape interval (default: 15 seconds)
- Executes multiple PromQL queries per scrape
- Preserves all Prometheus labels as InfluxDB tags
- Metric name becomes InfluxDB measurement
- Continuous polling loop with error recovery

**Command-line Arguments:**
```bash
--prometheus-url <url>          # Prometheus server (default: http://localhost:9090)
--influx-url <url>              # InfluxDB URL (default: http://localhost:8086)
--influx-org <org>              # InfluxDB organization (default: ucs)
--influx-bucket <bucket>        # Target bucket (default: prometheus)
--token <token>                 # InfluxDB authentication token (required)
--interval <seconds>            # Scrape interval (default: 15)
--queries <query1> <query2>...  # Custom PromQL queries (space-separated)
```

**Example:**
```bash
python prometheus_to_influx.py \
  --prometheus-url http://prometheus:9090 \
  --influx-url http://influxdb:8086 \
  --influx-org ucs \
  --influx-bucket prometheus \
  --token admintoken123 \
  --interval 30 \
  --queries 'up' 'rate(http_requests_total[5m])'
```

## Docker Compose Integration

### Environment Variables

Both containers require:
```yaml
INFLUX_TOKEN: ${INFLUX_TOKEN}           # InfluxDB authentication token
INFLUX_BUCKET: <bucket_name>            # Target bucket name
PYTHON_SCRIPT: <script_path>            # Which exporter to run
```


## Usage

### Building the Container

```bash
docker build -t influxdb-export:latest -f influxdb-export/Dockerfile .
```

### Running Locally (without Docker)

**Kafka Exporter:**
```bash
pip install kafka-python influxdb-client pandas

python influxdb-export/kafka_to_influx.py \
  --bootstrap localhost:9092 \
  --topic ics-sensor-data \
  --influx-url http://localhost:8086 \
  --influx-org ucs \
  --influx-bucket swat_db \
  --token admintoken123
```

**Prometheus Exporter:**
```bash
pip install prometheus-api-client influxdb-client

python influxdb-export/prometheus_to_influx.py \
  --prometheus-url http://localhost:9090 \
  --influx-url http://localhost:8086 \
  --influx-org ucs \
  --influx-bucket prometheus \
  --token admintoken123
```

### Running in Docker

```bash
# Start both exporters
docker-compose up -d kafka-to-influx prometheus-to-influx

# View logs
docker logs -f kafka-to-influx
docker logs -f prometheus-to-influx

# Debug connectivity
docker exec kafka-to-influx /app/debug.sh
docker exec prometheus-to-influx /app/debug.sh

# Stop exporters
docker-compose down
```

## Troubleshooting

### Kafka Exporter Issues

**No messages received:**
```bash
# Check Kafka connectivity
docker exec kafka-to-influx curl -v telnet://kafka:9092

# Check topic exists
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Check consumer group
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group kafka_to_influx_group
```

**InfluxDB write failures:**
```bash
# Test InfluxDB connection
curl http://localhost:8086/health

# Check bucket exists
docker exec influxdb influx bucket list

# Verify token
docker exec kafka-to-influx env | grep INFLUX_TOKEN
```

**Timestamp parsing errors:**
- Ensure CSV format matches: `DD/MM/YYYY HH:MM:SS AM/PM`
- Check for timezone issues
- Verify pandas datetime parsing

### Prometheus Exporter Issues

**No metrics collected:**
```bash
# Check Prometheus connectivity
curl http://localhost:9090/-/healthy

# Test PromQL query manually
curl 'http://localhost:9090/api/v1/query?query=up'

# Check scrape targets
curl http://localhost:9090/api/v1/targets
```

**Empty results:**
- Verify Prometheus has data to scrape
- Check query syntax
- Ensure job names match Prometheus configuration

### Container Issues

**Container not starting:**
```bash
# Check logs
docker logs kafka-to-influx

# Verify environment variables
docker exec kafka-to-influx env

# Test entrypoint manually
docker run -it --entrypoint /bin/bash influxdb-export:latest
```

**Line ending problems (Windows):**
```bash
# Rebuild with dos2unix
docker build --no-cache -f influxdb-export/Dockerfile .
```

## Data Model

### SWaT Sensor Data (from Kafka)

```
Measurement: swat_sensor_data
Tags:
  - status: "Normal" | "Attack"
Fields:
  - FIT101: <float>
  - LIT101: <float>
  - MV101: <float>
  - ... (all sensor readings)
Timestamp: Parsed from CSV
```

### Prometheus Metrics

```
Measurement: <metric_name>
Tags:
  - <label1>: <value1>
  - <label2>: <value2>
  - ... (all Prometheus labels)
Fields:
  - value: <float>
Timestamp: Scrape time (UTC)
```


## Security

### Token Management

**Use .env files:**
```bash
INFLUX_TOKEN='secret_token'
```

## Dependencies

```
kafka-python==2.0.2         # Kafka consumer client
influxdb-client             # InfluxDB 2.x client (synchronous writes)
pandas==2.2.2               # Timestamp parsing and data handling
prometheus-api-client       # Prometheus query client
```
