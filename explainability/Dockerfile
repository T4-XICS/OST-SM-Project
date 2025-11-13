# Minimal base image
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y procps curl && \
    rm -rf /var/lib/apt/lists/*

# Set workdir
WORKDIR /app

# Copy requirements and install dependencies
COPY explainability/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy explainability code
COPY explainability /app

# Run the explainability stream job
CMD ["python", "/app/stream_explain_job.py"]
