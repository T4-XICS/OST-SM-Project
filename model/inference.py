import logging
import os
import threading
import time
from collections import defaultdict
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, from_json, trim, when

import torch

from preprocess_data import preprocess_spark, create_dataloader
from network import LSTMVAE, load_model  # load_model should return a torch model instance
from evaluate import evaluate_lstm  # evaluate_lstm returns anomalies

from prometheus_client import start_http_server, Counter, Gauge, Histogram

# Configure logging to stdout so Promtail/Loki can scrape it
SERVICE_NAME = os.getenv("SERVICE_NAME", "spark-consumer")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format=f"%(asctime)s %(levelname)s %(name)s app={SERVICE_NAME} %(message)s",
)
logger = logging.getLogger(SERVICE_NAME)

# Gauge for anomalous sensor events
anomaly_gauge = Gauge(
    'anomalous_sensor_event',
    'Detected anomalous sensor event',
    ['sensor']
)

# Processing / model metrics
batches_processed = Counter('ae_batches_processed_total', 'Number of Spark batches processed')
batch_processing_time = Histogram('ae_batch_processing_seconds', 'Processing time per batch (s)')
model_reload_count = Counter('ae_model_reloads_total', 'Times the model was reloaded')
model_load_duration = Gauge('ae_model_load_duration_seconds', 'Duration of last model load (s)')

# Reconstruction / anomaly metrics
# per-sensor average reconstruction error (set to last observed)
recon_error = Histogram('ae_reconstruction_error', 'Distribution of reconstruction error', ['sensor'], buckets=(0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 50.0))
# number of anomalies detected in the latest batch per stage
anomaly_count = Gauge('ae_anomalies_in_batch', 'Number of anomalies detected in the batch', ['stage'])
# cumulative anomalies (sensor x stage)
anomaly_total = Counter('ae_anomalies_total', 'Cumulative number of anomalies', ['sensor', 'stage'])

# Throughput / readiness
rows_processed = Counter('ae_rows_processed_total', 'Total input rows processed')
sequences_ready = Gauge('ae_sequences_ready', 'Number of sequences available for inference in current batch')

# Optional: per-batch anomaly count summary
anomalies_in_last_batch = Gauge('ae_anomalies_last_batch', 'Total anomalies in last processed batch')

start_http_server(8001)
logger.info("Prometheus metrics server started on port 8001")

spark = (
    SparkSession.builder
    .appName("MockKafkaConsumerWithInference")
    .master("local[*]")
    .config("spark.hadoop.security.authentication", "simple")
    .getOrCreate()
)

# Used to aggregate anomalies by stage P1..P6.
sensor_to_stage = {
    # P1
    "FIT101":"P1","LIT101":"P1","MV101":"P1","P101":"P1","P102":"P1",
    # P2
    "AIT201":"P2","AIT202":"P2","AIT203":"P2","FIT201":"P2","MV201":"P2",
    # P3
    "DPIT301":"P3","FIT301":"P3","LIT301":"P3","MV301":"P3","MV302":"P3","MV303":"P3","MV304":"P3","P301":"P3","P302":"P3",
    # P4
    "AIT401":"P4","AIT402":"P4","FIT401":"P4","LIT401":"P4","P401":"P4","P402":"P4","P403":"P4","P404":"P4","UV401":"P4",
    # P5
    "AIT501":"P5","AIT502":"P5","AIT503":"P5","AIT504":"P5","FIT501":"P5","FIT502":"P5","FIT503":"P5","FIT504":"P5","P501":"P5","P502":"P5","PIT501":"P5","PIT502":"P5","PIT503":"P5",
    # P6
    "FIT601":"P6","P601":"P6","P602":"P6","P603":"P6"
}

# replace the previous typed schema with an all-string schema, then cast later
all_fields = [
    "Timestamp","FIT101","LIT101","MV101","P101","P102","AIT201","AIT202","AIT203",
    "FIT201","MV201","P201","P202","P203","P204","P205","P206","DPIT301","FIT301",
    "LIT301","MV301","MV302","MV303","MV304","P301","P302","AIT401","AIT402","FIT401",
    "LIT401","P401","P402","P403","P404","UV401","AIT501","AIT502","AIT503","AIT504",
    "FIT501","FIT502","FIT503","FIT504","P501","P502","PIT501","PIT502","PIT503",
    "FIT601","P601","P602","P603","Normal_Attack"
]

schema = StructType([StructField(f, StringType(), True) for f in all_fields])
numeric_cols = [c for c in all_fields if c not in ("Timestamp", "Normal_Attack")]

stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "ics-sensor-data")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)


device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
logger.info(f"Using device: {device}")

model_lock = threading.Lock()
current_model = None
model_path = "/weights/lstm_vae_swat.pth"
last_modified_time = None

def load_model_safe():
    global current_model, last_modified_time
    try:
        if not os.path.exists(model_path):
            logger.warning(f"Model path {model_path} does not exist yet.")
            return
        logger.info(f"Loading model from {model_path}")
        
        with model_lock:
            current_model = load_model(model_path, device=device)
            current_model.eval()
            last_modified_time = os.path.getmtime(model_path)
            logger.info(f"Model loaded successfully. Last modified: {last_modified_time}")
    except Exception as e:
        logger.error(f"Failed to load model: {e}")

def check_and_reload_model():
    global last_modified_time
    try:
        if not os.path.exists(model_path):
            return
        
        current_mtime = os.path.getmtime(model_path)
        if last_modified_time is None or current_mtime > last_modified_time:
            logger.info(f"Model weights updated (mtime: {current_mtime}). Reloading model...")
            load_model_safe()
        else:
            logger.info("Model weights have not changed; no reload needed.")
    except Exception as e:
        logger.error(f"Error checking model file: {e}")

def model_reload_worker(check_interval=60):
    logger.info(f"Model reload worker started. Check interval: {check_interval}s")
    while True:
        time.sleep(check_interval)
        check_and_reload_model()

load_model_safe()

# Start background thread for periodic model reloading
reload_thread = threading.Thread(target=model_reload_worker, args=(60,), daemon=True)
reload_thread.start()
logger.info("Model auto-reload thread started")

def _normalize_anomalies_output(anomalies):
    """
    Normalize return from evaluate_lstm into:
    - a list of anomalous indices (ints), and
    - an optional dict mapping idx -> recon_error (float)
    The evaluate_lstm implementation may vary; we try to be flexible.
    """
    if anomalies is None:
        return [], {}
    # If it's a dict mapping idx->error
    if isinstance(anomalies, dict):
        idxs = list(anomalies.keys())
        return idxs, anomalies
    # If it's a list of tuples (idx, err)
    if isinstance(anomalies, list) and len(anomalies) > 0 and isinstance(anomalies[0], (list, tuple)) and len(anomalies[0]) >= 2:
        idxs = [int(x[0]) for x in anomalies]
        errs = {int(x[0]): float(x[1]) for x in anomalies}
        return idxs, errs
    # If it's a plain list of indices
    if isinstance(anomalies, list):
        return [int(x) for x in anomalies], {}
    # Unknown type: fallback
    logger.warning(f"Unexpected anomalies type: {type(anomalies)}")
    return [], {}





def run_eval(batch_df, batch_id):
    if batch_df.count() == 0:
        logger.info(f"Batch {batch_id} is empty. Skipping evaluation.")
        return

    # Wrap processing in histogram timer and increment batches_processed
    with batch_processing_time.time():
        batches_processed.inc()
        try:
            logger.info(f"Batch {batch_id} RAW SAMPLE (value column)")
            batch_df.select(col("value").cast("string").alias("raw_value")).show(5, False)

            # parse JSON into string columns
            cleaned = batch_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

            # trim and cast numeric string columns -> DoubleType, treat empty strings as null
            parsed = cleaned
            for c in numeric_cols:
                parsed = parsed.withColumn(c, when(trim(col(c)) == "", None).otherwise(col(c).cast(DoubleType())))

            # trim timestamp and label
            parsed = parsed.withColumn("Timestamp", trim(col("Timestamp")))\
                        .withColumn("Normal_Attack", trim(col("Normal_Attack")))

            logger.info(f"Batch {batch_id} PARSED SAMPLE")
            parsed.show(5, False)

            total = parsed.count()
            logger.info(f"Parsed rows: {total}")
            rows_processed.inc(total)

            for f in parsed.columns:
                nulls = parsed.filter(col(f).isNull()).count()
                logger.debug(f"{f}: nulls={nulls}")

            # only then preprocess and create dataloader
            df_pre = preprocess_spark(parsed)
            dataloader = create_dataloader(df_pre, batch_size=32, sequence_length=30)

            if dataloader is None:
                logger.info(f"Batch {batch_id}: not enough data to run inference yet. Skipping.")
                return

            df_pre.show(5, False)

            with model_lock:
                if current_model is None:
                    logger.warning(f"Batch {batch_id}: Model not loaded yet. Skipping evaluation.")
                    return
                model_to_use = current_model

            # Run evaluation (expects evaluate_lstm to return anomalies)
            logger.info(f"Running evaluate_lstm on batch {batch_id} ...")
            anomalies_raw = evaluate_lstm(model_to_use, dataloader, device, int(os.getenv("ANOMALY_PERCENTILE", "90")))
            # Normalize anomalies output to idx list + optional errors dict
            anomalies_idx_list, anomalies_with_errors = _normalize_anomalies_output(anomalies_raw)
            

            logger.info(f"Batch {batch_id} Evaluation result - anomalous indices: {anomalies_idx_list}")

            # Update Prometheus with anomaly details
            # Reset per-batch stage counts
            stage_counts = defaultdict(int)
            total_anomalies_in_batch = 0

            if len(anomalies_idx_list) > 0:
                # If evaluate_lstm returned errs mapping, use it; otherwise set NaN or 1.0
                for idx in anomalies_idx_list:
                    if idx is None:
                        continue
                    total_anomalies_in_batch += 1
                    if idx < 0 or idx >= len(numeric_cols):
                        logger.warning(f"Anomaly index {idx} out of range for sensors list")
                        continue
                    sensor = numeric_cols[idx]
                    stage = sensor_to_stage.get(sensor, "unknown")
                    stage_counts[stage] += 1

                    # set recon_error if provided
                    err_val = anomalies_with_errors.get(idx, None)
                    if err_val is None:
                        # if not provided, set a nominal value of 1.0 (or could compute from model)
                        try:
                            recon_error.labels(sensor=sensor).set(1.0)
                        except Exception:
                            pass
                    else:
                        # Observe the actual reconstruction error value
                        try:
                            # Observation for the Histogram
                            recon_error.labels(sensor=sensor).observe(float(err_val))
                        except Exception:
                            pass

                    # increment cumulative counter
                    try:
                        anomaly_total.labels(sensor=sensor, stage=stage).inc()
                    except Exception as e:
                        logger.debug(f"Failed to increment anomaly_total for {sensor}:{stage}: {e}")

                    # For the anomalous sensor, try to extract the latest value for reporting via anomaly_gauge
                    # We attempt to fetch last row from df_pre or dataloader dataset
                    try:
                        # dataloader.dataset expected to be e.g., pandas DataFrame or numpy/tensor sequence list
                        data_container = dataloader.dataset
                        if hasattr(data_container, 'iloc'):
                            tensor_seq = data_container.iloc[-1]  # last sequence
                        else:
                            tensor_seq = data_container[-1]
                        # attempt to index into last time step
                        if hasattr(tensor_seq, 'shape') and len(tensor_seq.shape) == 2:
                            seq_len, num_sensors = tensor_seq.shape
                            if idx < num_sensors:
                                value = tensor_seq[-1, idx]
                                # if tensor
                                try:
                                    val = float(value)
                                except Exception:
                                    # if torch tensor
                                    try:
                                        val = float(value.item())
                                    except Exception:
                                        val = None
                                if val is not None:
                                    anomaly_gauge.labels(sensor=sensor).set(val)
                                    logger.debug(f"Set anomaly_gauge for {sensor} to {val}")
                                else:
                                    anomaly_gauge.labels(sensor=sensor).set(0)
                            else:
                                anomaly_gauge.labels(sensor=sensor).set(0)
                        else:
                            # fallback: set 1 if anomaly occurred
                            anomaly_gauge.labels(sensor=sensor).set(1)
                    except Exception as e:
                        logger.debug(f"Could not set anomaly_gauge for idx {idx}: {e}")
            else:
                # No anomalies => reset recon_error for all sensors to 0 and gauges to 0
                for sensor in numeric_cols:
                    try:
                        recon_error.labels(sensor=sensor).set(0)
                        anomaly_gauge.labels(sensor=sensor).set(0)
                    except Exception:
                        pass

            # Set per-stage and batch-level metrics
            for st in ("P1","P2","P3","P4","P5","P6","unknown"):
                cnt = stage_counts.get(st, 0)
                try:
                    anomaly_count.labels(stage=st).set(cnt)
                except Exception:
                    pass

            anomalies_in_last_batch.set(total_anomalies_in_batch)

            logger.info(f"Batch {batch_id} anomaly stage counts: {dict(stage_counts)}")
            logger.info(f"Batch {batch_id}: total anomalies detected = {total_anomalies_in_batch}")

        except Exception as e:
            logger.exception(f"Error during run_eval for batch {batch_id}: {e}")

query = (
    stream_df
    .writeStream
    .foreachBatch(lambda df, batch_id: run_eval(df, batch_id))
    #.option("checkpointLocation", "checkpoints/mock_consumer")
    .start()
)

logger.info("Spark streaming started - awaiting termination")
query.awaitTermination()
