# Base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install deps
RUN pip install --no-cache-dir kafka-python prometheus_client

# Copy producer code
COPY producer/send_csv_to_kafka.py /app/send_csv_to_kafka.py

# Bake the CSV into the image at the same path used by the script
COPY datasets/swat/normal/SWaT_Dataset_Normal_v0_1.csv /app/SWaT_Dataset_Normal_v0_1.csv

# Default command
CMD ["python", "/app/send_csv_to_kafka.py"]
