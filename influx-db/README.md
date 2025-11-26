# InfluxDB Module

This module provides util tools and functions to work with the InfluxDB database of the project.

## Overview

The module contains utilities for:
- Querying InfluxDB databases (both v2.x and v3.x)
- Writing streaming data from Kafka to InfluxDB
- Checking database status and data integrity
- Analyzing attack/anomaly data
- Mock data generation for testing

## Architecture

The system supports two InfluxDB instances:
- **Port 8086**: InfluxDB 2.x - Contains monitoring metrics and Prometheus data + SWaT sensor data from Kafka streams

## Configuration

Default configuration values:

```python
INFLUX_ORG = "ucs"
INFLUX_URL_V2 = "http://localhost:8086"
INFLUX_TOKEN = "admintoken123"
INFLUX_BUCKET = "swat_db"  # or "prometheus" for metrics
```

For easier usage, set `INFLUX_TOKEN` via environment variable.

## Files

### Core Scripts

#### `check_influx.py`
Simple diagnostic tool to check InfluxDB connection, list buckets, and summarize data.

**Usage:**
```bash
python check_influx.py
```

**Features:**
- Lists all buckets in the organization
- Shows measurements per bucket
- Counts records (handles historical data from 2015)
- Provides connection diagnostics

#### `query_influx.py`
Main query interface for SWaT sensor data with multiple query modes.

**Usage:**
```bash
python query_influx.py
```

**Key Functions:**
- `query_swat_data(limit, time_range, measurement, status_filter)` - Generic query
- `get_latest_records(limit)` - Get most recent data
- `get_sensor_data(sensor_name, limit)` - Query specific sensor
- `get_attack_data(limit)` - Get only attack/anomaly records

**Example:**
```python
from query_influx import get_attack_data, get_sensor_data

# Get attack records
attacks = get_attack_data(limit=100)

# Get specific sensor data
sensor_data = get_sensor_data("FIT101", limit=200)
```

#### `attack_influx_query.py`
Specialized queries for analyzing attack data patterns.

**Usage:**
```bash
python attack_influx_query.py
```

**Provides:**
- Count by status (Normal vs Attack)
- Sample attack records
- Attack timeline analysis
- Top sensors with attack data

#### `query_prometheus_influxdb.py`
Query tool for the Prometheus metrics bucket.

**Usage:**
```bash
python query_prometheus_influxdb.py
```

Automatically tries multiple time ranges to find data.

### Data Pipeline

#### `write_to_influx_db.py`
PySpark streaming application that reads from Kafka and writes to InfluxDB v3.

**Features:**
- Consumes from Kafka topic `logs`
- Batched writes with configurable backpressure
- Error handling with retry logic
- Callbacks for success/error monitoring

**Schema:**
```python
{
    "timestamp": str,
    "host": str,
    "service": str,
    "level": str,
    "message": str,
    "anomaly_score": float
}
```

**Configuration:**
- Batch size: 500 points
- Flush interval: 10 seconds
- Max retries: 5
- Exponential backoff

**Usage:**
```bash
source .venv/bin/activate
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    write_to_influx_db.py
```

### Testing

#### `mock_log_generator.py`
Generates synthetic log data for testing the pipeline.

**Usage:**
```bash
python mock_log_generator.py
```

**Generates:**
- Random service logs (auth, payment, orders, inventory)
- Log levels (INFO, WARN, ERROR)
- Anomaly scores
- Publishes to Kafka topic `logs`

## Dependencies

See `requirements.txt` for full list:

```
pyspark==3.5.1
kafka-python==2.0.2
influxdb3-python==0.3.0
confluent-kafka==2.12.1
influxdb-client (for v2.x)
pandas==2.2.2
numpy==1.26.4
```

Install with:
```bash
pip install -r requirements.txt
```

## Data Model

### SWaT Sensor Data
- **Measurement**: `swat_sensor_data`
- **Fields**: Sensor readings (FIT101, LIT101, etc.)
- **Tags**: `status` (Normal/Attack)
- **Timestamp**: Historical data from 2015

### Log Anomaly Data
- **Measurement**: `log_anomaly`
- **Fields**: `message`, `anomaly_score`
- **Tags**: `service`, `level`, `host`
- **Timestamp**: Stream time
  

## Troubleshooting

### No data found
- SWaT data is historical (from 2015), use `start: 2015-01-01T00:00:00Z`
- Check container is running: `docker ps`
- Verify bucket exists: `python check_influx.py`

### Connection refused
- Ensure InfluxDB is running on correct port
- Check token: should be `admintoken123` from .env
- Verify network: `curl http://localhost:8086/health`

### Empty prometheus bucket
- Check `prometheus-to-influx` container logs
- Verify Prometheus is scraping metrics
- Ensure write permissions on bucket

## Web UI Access

InfluxDB v2.x UI: http://localhost:8086
- Login: `admin` / `adminpass`
- Browse buckets, run queries, view dashboards
