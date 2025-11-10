#!/bin/bash
set -e

if [[ "$PYTHON_SCRIPT" == *kafka_to_influx.py ]]; then
    exec python -u "$PYTHON_SCRIPT" \
        --bootstrap kafka:9092 \
        --topic ics-sensor-data \
        --influx-url http://influxdb:8086 \
        --influx-org ucs \
        --influx-bucket "$INFLUX_BUCKET" \
        --token "$INFLUX_TOKEN"
elif [[ "$PYTHON_SCRIPT" == *prometheus_to_influx.py ]]; then
    exec python -u "$PYTHON_SCRIPT" \
        --prometheus-url http://prometheus:9090 \
        --influx-url http://influxdb:8086 \
        --influx-org ucs \
        --influx-bucket "$INFLUX_BUCKET" \
        --token "$INFLUX_TOKEN" \
        --interval 15
fi
