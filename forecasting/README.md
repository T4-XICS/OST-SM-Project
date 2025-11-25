# Forecasting Service

This service is responsible for running the forecasting pipeline using Spark Streaming and writing predictions to InfluxDB.

## How to Build and Run the Docker Container

### Prerequisites
- Docker installed on your system
- Kafka and InfluxDB services running

### Steps

1. **Build the Docker Image**
   ```bash
   docker build -t forecasting-service .
   ```

2. **Run the Docker Container**
   ```bash
   docker run --name forecasting-service \
     --network="host" \
     -e KAFKA_BOOTSTRAP_SERVERS="localhost:9092" \
     -e INFLUXDB_URL="http://localhost:8086" \
     -e INFLUXDB_TOKEN="your-influxdb-token" \
     -e INFLUXDB_ORG="your-org" \
     -e INFLUXDB_BUCKET="forecasting" \
     forecasting-service
   ```

### Environment Variables
- `KAFKA_BOOTSTRAP_SERVERS`: The Kafka bootstrap servers (e.g., `localhost:9092`)
- `INFLUXDB_URL`: The URL of the InfluxDB instance (e.g., `http://localhost:8086`)
- `INFLUXDB_TOKEN`: The authentication token for InfluxDB
- `INFLUXDB_ORG`: The organization name in InfluxDB
- `INFLUXDB_BUCKET`: The bucket name in InfluxDB

### Logs
To view logs from the container, run:
```bash
docker logs forecasting-service
```

### Stopping the Container
To stop the container, run:
```bash
docker stop forecasting-service
```

To remove the container, run:
```bash
docker rm forecasting-service
```