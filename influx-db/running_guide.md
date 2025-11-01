For running the code execute the following steps:

## Requirements
### System Requirements

* **Python 3.10+**
* **Java 8+ or 11+**
* **Apache Kafka** (downloaded and extracted)
* **Apache Spark** (with PySpark)
* **InfluxDB 3 Core (local server)**

### Python Dependencies

Install with:

```bash
pip install -r requirements.txt
```

### Set Environment Variables

Create a `.env` file in your project root:

```bash
INFLUX_HOST=http://localhost:8181
INFLUX_TOKEN=secret-token
INFLUX_DATABASE=test
KAFKA_BROKER=localhost:9092
KAFKA_TOPIC=log_topic
```

Export them (for local shell):

```bash
export $(grep -v '^#' .env | xargs)
```


## Required Services

### Start ZooKeeper

If you have Kafka installed in `~/kafka`, run:

```bash
cd ~/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Keep this running (use a new terminal for the next steps).

---

### Start Kafka Broker

Open a **new terminal**, then:

```bash
cd ~/kafka
bin/kafka-server-start.sh config/server.properties
```

---

### Create Kafka Topic (once)

```bash
bin/kafka-topics.sh --create --topic log_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

Verify:

```bash
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

You should see:

```
log_topic
```

---

### Start InfluxDB 3 (Local)

```bash
influxdb3 serve \
  --object-store file \
  --node-id host01 \
  --data-dir ~/.influxdb3
```

In another terminal, create your database (bucket):

```bash
influxdb3 create token --admin 
```
The output will be a **secret-token** which you should store somewhere.

```bash
influxdb3 create database test --token secret-token
```

Check it:

```bash
influxdb3 show databases --token secret-token
```

##  Run the Pipeline

### Start Mock Log Generator

This script produces fake log data into Kafka:

```bash
python influx-db/mock_log_generator.py
```

### Start PySpark Stream into InfluxDB

Now run the Spark script to consume Kafka data and store it into InfluxDB 3:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 influx-db/write_to_influx_db.py
```

```bash
python spark_kafka_to_influx.py
```

### Verify Data in InfluxDB

Run:

```bash
influxdb3 query --database test --token secret-token 'SELECT * FROM log_anomaly;'
```

## Troubleshooting

| Problem                                | Likely Cause        | Fix                                    |
| -------------------------------------- | ------------------- | -------------------------------------- |
| `Connection refused to localhost:9092` | Kafka not running   | Start ZooKeeper & Kafka first          |
| `table 'logs' not found`               | No data written yet | Run mock_log_generator first           |
| `Failed to connect to InfluxDB`        | Wrong port or token | Check `INFLUX_HOST` and `INFLUX_TOKEN` |
| `Spark app stuck at starting`          | Kafka config error  | Verify topic name and broker address   |