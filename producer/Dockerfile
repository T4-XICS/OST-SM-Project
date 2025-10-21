# Base image
FROM python:3.8-slim

# Set working directory
WORKDIR /app

# Copy files into container
COPY requirements.txt .
COPY send_csv_to_kafka.py .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set default command
CMD ["python", "send_csv_to_kafka.py"]
