# Dockerfile for Kafka to InfluxDB consumer
FROM python:3.11-slim

WORKDIR /app

# Install system utilities for debugging and dependencies
RUN apt-get update && \
    apt-get install -y procps curl && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir kafka-python influxdb-client pandas

# Copy consumer script
COPY consumer/kafka_to_influx.py /app/kafka_to_influx.py

# Add debugging script
RUN echo '#!/bin/bash\necho "KAFKA_SERVER=kafka:9092"\necho "INFLUX_URL=http://influxdb:8086"\necho "Current processes:"\nps aux\necho "\nPython processes:"\nps aux | grep python\necho "\nNetwork connectivity:"\necho "Kafka:"\ncurl -v telnet://kafka:9092 2>&1 | grep "Connected"\necho "InfluxDB:"\ncurl -v http://influxdb:8086/health 2>&1 | grep -E "Connected|status"\n' > /app/debug.sh && \
    chmod +x /app/debug.sh

# Default command - use shell form to avoid issues with multi-line
CMD python /app/kafka_to_influx.py \
    --bootstrap kafka:9092 \
    --topic ics-sensor-data \
    --influx-url http://influxdb:8086 \
    --influx-org ucs \
    --influx-bucket swat_db \
    --token $INFLUX_TOKEN
