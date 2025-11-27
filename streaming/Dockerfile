# Base image with Python
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
        pandas \
        influxdb-client \
        python-dotenv

# Copy your streaming scripts
COPY streaming/ /app/streaming/
COPY datasets/swat/normal/SWaT_Dataset_Normal_v0_1.csv /app/datasets/swat/normal/SWaT_Dataset_Normal_v0_1.csv

# Environment variables for InfluxDB (no manual PowerShell export!)
ENV INFLUX_URL=http://influxdb:8086
ENV INFLUX_TOKEN="${INFLUXDB_TOKEN}"
ENV INFLUX_ORG=ucs
ENV INFLUX_BUCKET=swat_db

# Entry point (runs your script automatically)
CMD ["python", "streaming/event_pattern_mining.py", \
     "--test-file", "datasets/swat/normal/SWaT_Dataset_Normal_v0_1.csv"]
