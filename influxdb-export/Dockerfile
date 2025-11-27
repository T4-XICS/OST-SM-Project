# Dockerfile for Kafka to InfluxDB consumer
FROM python:3.11-slim

WORKDIR /app

# Install system utilities for debugging and dependencies
RUN apt-get update && \
    apt-get install -y procps curl bash dos2unix && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir kafka-python influxdb-client pandas prometheus-api-client

# Copy scripts
COPY influxdb-export/kafka_to_influx.py /app/kafka_to_influx.py
COPY influxdb-export/prometheus_to_influx.py /app/prometheus_to_influx.py

# Add debugging script
RUN echo '#!/bin/bash\necho "KAFKA_SERVER=kafka:9092"\necho "INFLUX_URL=http://influxdb:8086"\necho "Current processes:"\nps aux\necho "\nPython processes:"\nps aux | grep python\necho "\nNetwork connectivity:"\necho "Kafka:"\ncurl -v telnet://kafka:9092 2>&1 | grep "Connected"\necho "InfluxDB:"\ncurl -v http://influxdb:8086/health 2>&1 | grep -E "Connected|status"\n' > /app/debug.sh && \
    chmod +x /app/debug.sh

COPY influxdb-export/entrypoint.sh /app/entrypoint.sh
RUN dos2unix /app/entrypoint.sh && chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]