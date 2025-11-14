# Minimal base image
FROM python:3.11-slim

# Install system dependencies: Java (for PySpark) + ps (procps) + curl
RUN apt-get update && \
    apt-get install -y openjdk-21-jre-headless procps curl && \
    rm -rf /var/lib/apt/lists/*

# Workdir for the app
WORKDIR /app

# 1) Copy explainability package into the container
COPY explainability /app/explainability

# 2) Install Python dependencies
RUN pip install --no-cache-dir -r /app/explainability/requirements.txt

# 3) Add Spark + Kafka connector JARs so that format("kafka") works
RUN mkdir -p /usr/local/lib/python3.11/site-packages/pyspark/jars && \
    curl -L -o /usr/local/lib/python3.11/site-packages/pyspark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -L -o /usr/local/lib/python3.11/site-packages/pyspark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    curl -L -o /usr/local/lib/python3.11/site-packages/pyspark/jars/kafka-clients-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    curl -L -o /usr/local/lib/python3.11/site-packages/pyspark/jars/commons-pool2-2.11.1.jar \
        https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# 4) Run the stream job as a module so that "explainability" is a proper package
CMD ["python", "-m", "explainability.stream_explain_job"]
