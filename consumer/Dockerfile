# consumer/Dockerfile
FROM apache/spark:3.5.1

USER root

RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -L -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    curl -L -o /opt/spark/jars/kafka-clients-3.5.1.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    curl -L -o /opt/spark/jars/commons-pool2-2.11.1.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

WORKDIR /app
COPY kafka_spark_stream.py .

# Properly pass JARs to Spark
CMD ["/opt/spark/bin/spark-submit", "--master", "local[*]", \
     "--jars", "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,/opt/spark/jars/kafka-clients-3.6.1.jar,/opt/spark/jars/commons-pool2-2.11.1.jar", \
     "/app/kafka_spark_stream.py"]

