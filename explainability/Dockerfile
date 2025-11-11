# Minimal, reliable base
FROM python:3.11-slim

# Java is required by PySpark
RUN apt-get update && \
    apt-get install -y procps curl && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir kafka-python influxdb-client pandas

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"


# App code
WORKDIR /app
COPY explainability /app

# Run your Python script (no need for spark-submit in local mode)
CMD ["python", "/app/stream_explain_job.py"]


