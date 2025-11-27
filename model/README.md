# AutEncoder Model

This subproject contains files related to the AutoEncoder model.

## How to run the code

### Run the app

#### Train

```bash
kubectl apply -f train.yaml
```

#### Inference

```bash
kubectl apply -f inference.yaml
```

### Run the app locally

First, the python virtual environment needs to be created with the all of the necessary dependencies installed.

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Export the correct Java version before starting the Spark apps if the default one is incorrect.

```bash
export JAVA_HOME=`/usr/libexec/java_home -v 17`
```

After that, the train and inference apps can be run by using `spark-submit`.

#### Train

```bash
spark-submit train.py
```

#### Inference

```bash
spark-submit inference.py
```

## Prometheus Reporting for Anomalies

The inference service exposes detected anomalous sensor data as Prometheus metrics on port 8001. Each detected anomaly sets the `anomalous_sensor_event` gauge for the affected sensors.

#### How to use

1. Ensure the model container is running and port 8001 is accessible to Prometheus.
2. Add the following scrape config to your Prometheus configuration:

    ```yaml
        - job_name: 'model-anomaly-inference'
            static_configs:
                - targets: ['model:8001']  # Replace 'model' with the actual service name or IP
    ```

3. The metric `anomalous_sensor_event{sensor="SENSOR_NAME"}` will be set to the sensor value when an anomaly is detected, or 0 otherwise.

## Grafana Dashboard

A ready-to-import Grafana dashboard is provided at:
`deployment/grafana/provisioning/dashboards/anomalous_sensor_events.json`

This dashboard visualizes anomalous sensor events over time for all monitored sensors, making it easy for operators to interpret and respond to detected anomalies.

---

##  Additional Prometheus Metrics Exposed by the Inference Service

Beyond `anomalous_sensor_event`, the service exports several metrics for monitoring anomaly detection, processing performance, and reconstruction error distributions. All metrics are emitted by the anomaly-inference pipeline.

---

##  Anomaly & Reconstruction Metrics

### **`ae_reconstruction_error{sensor=...}`**  
**Type:** Histogram  
Tracks reconstruction error distributions from the LSTM-VAE model.  
Buckets: `0.01, 0.05, 0.1, 0.5, 1, 5, 10, 50`.

### **`ae_anomalies_in_batch{stage=...}`**  
**Type:** Gauge  
Number of anomalies detected in the **current batch**, grouped by SWaT stage (`P1–P6`).

### **`ae_anomalies_total{sensor=..., stage=...}`**  
**Type:** Counter  
Cumulative anomaly count per sensor and per stage.

### **`ae_anomalies_last_batch`**  
**Type:** Gauge  
Total anomalies detected in the **latest inference batch**.

---

##  Processing & Model Lifecycle Metrics

### **`ae_batches_processed_total`**  
**Type:** Counter  
Total number of micro-batches processed by the inference service.

### **`ae_batch_processing_seconds`**  
**Type:** Histogram  
Time taken to process a batch, including preprocessing and inference.

### **`ae_model_reloads_total`**  
**Type:** Counter  
Number of times the `.pth` model file has been reloaded.

### **`ae_model_load_duration_seconds`**  
**Type:** Gauge  
Duration (in seconds) of the most recent model load.

---

##  Throughput & Readiness Metrics

### **`ae_rows_processed_total`**  
**Type:** Counter  
Total number of rows processed by the streaming service.

### **`ae_sequences_ready`**  
**Type:** Gauge  
Number of sequences prepared for inference after preprocessing.  
If too low, the model will skip inference for that batch.

---

## Stage Mapping Logic

Internal logic maps each sensor to a SWaT stage (`P1–P6`), enabling Prometheus and Grafana dashboards to show:

- per-stage anomaly counts  
- anomaly propagation patterns  
- upstream/downstream relationships  

---

