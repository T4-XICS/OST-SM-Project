import logging
import os
import threading
import time
from collections import defaultdict
import numpy as np
import joblib

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

# OCSVM processing / metrics
ocsvm_batches_processed = Counter('ocsvm_batches_processed_total', 'Number of batches scored by OCSVM')
ocsvm_anomalies_in_batch = Gauge('ocsvm_anomalies_in_batch', 'Number of anomalies detected by OCSVM in batch')
ocsvm_anomalies_by_stage = Gauge('ocsvm_anomalies_in_batch_by_stage', 'Number of anomalies detected by OCSVM in batch, by stage', ['stage'])
ocsvm_anomaly_total = Counter('ocsvm_anomalies_total', 'Total anomalies detected by OCSVM')
ocsvm_anomaly_total_by_stage = Counter('ocsvm_anomalies_total_by_stage', 'Cumulative OCSVM anomalies by stage', ['stage'])
ocsvm_model_reload_count = Counter('ocsvm_model_reloads_total', 'Times the OCSVM artifacts were reloaded')
ocsvm_model_load_duration = Gauge('ocsvm_model_load_duration_seconds', 'Duration of last OCSVM model+scaler load (s)')
ocsvm_rows_processed = Counter('ocsvm_rows_processed_total', 'Total rows processed by OCSVM pipeline')
ocsvm_ready = Gauge('ocsvm_ready', 'OCSVM artifacts are loaded and scoring is enabled (1=yes)')
ocsvm_anomalies_last_batch = Gauge('ocsvm_anomalies_last_batch', 'Total anomalies detected by OCSVM in last processed batch')
# Initialize OCSVM gauges so they always appear on /metrics even before first batch
ocsvm_ready.set(0)
ocsvm_anomalies_in_batch.set(0)
ocsvm_anomalies_last_batch.set(0)
for _st in ("P1","P2","P3","P4","P5","P6","unknown"):
    ocsvm_anomalies_by_stage.labels(stage=_st).set(0)
    ocsvm_anomaly_total_by_stage.labels(stage=_st).inc(0)
ocsvm_anomaly_total.inc(0)

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

ocsvm_lock = threading.Lock()
ocsvm_model = None
ocsvm_scaler = None
_default_ocsvm_model = "/weights/OCSVM/ocsvm_model.pkl"
_default_ocsvm_scaler = "/weights/OCSVM/scaler.pkl"
# also try bundled relative paths as fallbacks
_relative_ocsvm_model = os.path.join(os.path.dirname(__file__), "OCSVM", "ocsvm_model.pkl")
_relative_ocsvm_scaler = os.path.join(os.path.dirname(__file__), "OCSVM", "scaler.pkl")

def _resolve_ocsvm_paths():
    """Pick existing paths in priority: env -> /weights -> bundled relative."""
    env_model = os.getenv("OCSVM_MODEL_PATH")
    env_scaler = os.getenv("OCSVM_SCALER_PATH")
    candidates_model = [p for p in [env_model, _default_ocsvm_model, _relative_ocsvm_model] if p]
    candidates_scaler = [p for p in [env_scaler, _default_ocsvm_scaler, _relative_ocsvm_scaler] if p]

    chosen_model = next((p for p in candidates_model if os.path.exists(p)), candidates_model[0])
    chosen_scaler = next((p for p in candidates_scaler if os.path.exists(p)), candidates_scaler[0])
    return chosen_model, chosen_scaler

ocsvm_model_path, ocsvm_scaler_path = _resolve_ocsvm_paths()
ocsvm_last_mtime = (None, None)

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

def load_ocsvm_safe():
    """Load OCSVM model + scaler with locking and timing."""
    global ocsvm_model, ocsvm_scaler, ocsvm_last_mtime
    start = time.time()
    try:
        model_path_candidate, scaler_path_candidate = _resolve_ocsvm_paths()
        if not (os.path.exists(model_path_candidate) and os.path.exists(scaler_path_candidate)):
            logger.warning(f"OCSVM artifacts missing: model={model_path_candidate}, scaler={scaler_path_candidate}")
            ocsvm_ready.set(0)
            return

        with ocsvm_lock:
            ocsvm_model = joblib.load(model_path_candidate)
            ocsvm_scaler = joblib.load(scaler_path_candidate)
            ocsvm_model_path, ocsvm_scaler_path = model_path_candidate, scaler_path_candidate
            ocsvm_last_mtime = (os.path.getmtime(model_path_candidate), os.path.getmtime(scaler_path_candidate))

        ocsvm_model_reload_count.inc()
        duration = time.time() - start
        ocsvm_model_load_duration.set(duration)
        logger.info(f"OCSVM artifacts loaded in {duration:.3f}s (model={ocsvm_model_path}, scaler={ocsvm_scaler_path}, mtime={ocsvm_last_mtime})")
        ocsvm_ready.set(1)
    except Exception as e:
        logger.error(f"Failed to load OCSVM artifacts: {e}")
        ocsvm_ready.set(0)

def check_and_reload_ocsvm():
    """Reload OCSVM artifacts if either file has changed."""
    global ocsvm_last_mtime
    try:
        # re-resolve paths in case env vars change between checks
        model_path_candidate, scaler_path_candidate = _resolve_ocsvm_paths()
        if not (os.path.exists(model_path_candidate) and os.path.exists(scaler_path_candidate)):
            return

        model_mtime = os.path.getmtime(model_path_candidate)
        scaler_mtime = os.path.getmtime(scaler_path_candidate)
        if ocsvm_last_mtime == (None, None) or model_mtime > ocsvm_last_mtime[0] or scaler_mtime > ocsvm_last_mtime[1]:
            logger.info(f"OCSVM artifacts updated (model={model_path_candidate}, scaler={scaler_path_candidate}, mtime=({model_mtime},{scaler_mtime})). Reloading...")
            load_ocsvm_safe()
    except Exception as e:
        logger.error(f"Error checking OCSVM files: {e}")

def ocsvm_reload_worker(check_interval=60):
    logger.info(f"OCSVM reload worker started. Check interval: {check_interval}s")
    while True:
        time.sleep(check_interval)
        check_and_reload_ocsvm()

load_model_safe()
load_ocsvm_safe()

# Start background thread for periodic model reloading
reload_thread = threading.Thread(target=model_reload_worker, args=(60,), daemon=True)
reload_thread.start()
logger.info("Model auto-reload thread started")

ocsvm_reload_thread = threading.Thread(target=ocsvm_reload_worker, args=(60,), daemon=True)
ocsvm_reload_thread.start()
logger.info("OCSVM auto-reload thread started")

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

def run_ocsvm_pipeline(df_pre_pd, batch_id):
    """
    Run the OCSVM model on a pandas DataFrame (row-wise) and report metrics.
    """
    if df_pre_pd is None or len(df_pre_pd.index) == 0:
        logger.info(f"Batch {batch_id}: no rows available for OCSVM scoring.")
        ocsvm_anomalies_in_batch.set(0)
        ocsvm_anomalies_last_batch.set(0)
        for st in ("P1","P2","P3","P4","P5","P6","unknown"):
            ocsvm_anomalies_by_stage.labels(stage=st).set(0)
        return

    # Always record rows and batch attempts so the metrics move even if artifacts are missing
    rows_in_batch = len(df_pre_pd.index)
    ocsvm_rows_processed.inc(rows_in_batch)
    ocsvm_batches_processed.inc()

    with ocsvm_lock:
        if ocsvm_model is None or ocsvm_scaler is None:
            logger.info(f"Batch {batch_id}: OCSVM artifacts not loaded (model={ocsvm_model_path}, scaler={ocsvm_scaler_path}); skipping OCSVM scoring.")
            ocsvm_anomalies_in_batch.set(0)
            ocsvm_anomalies_last_batch.set(0)
            for st in ("P1","P2","P3","P4","P5","P6","unknown"):
                ocsvm_anomalies_by_stage.labels(stage=st).set(0)
            ocsvm_ready.set(0)
            return
        model = ocsvm_model
        scaler = ocsvm_scaler

    try:
        features = df_pre_pd.to_numpy(dtype=np.float32)
        scaled = scaler.transform(features)
        preds = model.predict(scaled)
        anomaly_rows = [int(i) for i, p in enumerate(preds) if p == -1]

        ocsvm_anomalies_in_batch.set(len(anomaly_rows))
        ocsvm_anomalies_last_batch.set(len(anomaly_rows))
        stage_counts = defaultdict(int)
        if anomaly_rows:
            ocsvm_anomaly_total.inc(len(anomaly_rows))
            for row_idx in anomaly_rows:
                try:
                    # pick the sensor with highest absolute z-score in the scaled vector as the "responsible" sensor
                    max_col = int(np.argmax(np.abs(scaled[row_idx])))
                    if 0 <= max_col < len(numeric_cols):
                        sensor_name = numeric_cols[max_col]
                        stage = sensor_to_stage.get(sensor_name, "unknown")
                    else:
                        stage = "unknown"
                except Exception:
                    stage = "unknown"

                stage_counts[stage] += 1
                ocsvm_anomaly_total_by_stage.labels(stage=stage).inc()

        for st in ("P1","P2","P3","P4","P5","P6","unknown"):
            ocsvm_anomalies_by_stage.labels(stage=st).set(stage_counts.get(st, 0))

        logger.info(f"Batch {batch_id}: OCSVM anomalies at row indices {anomaly_rows}")
    except Exception as e:
        logger.exception(f"Batch {batch_id}: error during OCSVM scoring: {e}")
        # ensure gauge is cleared on failure
        ocsvm_anomalies_in_batch.set(0)
        ocsvm_anomalies_last_batch.set(0)
        for st in ("P1","P2","P3","P4","P5","P6","unknown"):
            ocsvm_anomalies_by_stage.labels(stage=st).set(0)





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
            # Convert once for both pipelines (pandas DataFrame)
            df_pre_pd = df_pre.toPandas()
            dataloader = create_dataloader(df_pre_pd, batch_size=32, sequence_length=30)

            if dataloader is None:
                logger.info(f"Batch {batch_id}: not enough data to run VAE inference yet.")

            df_pre.show(5, False)

            # Run OCSVM pipeline (row-wise, no sequences needed)
            run_ocsvm_pipeline(df_pre_pd, batch_id)

            if dataloader is None:
                # We still ran OCSVM; skip the VAE path for this batch.
                return

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
                            recon_error.labels(sensor=sensor).observe(1.0)
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
