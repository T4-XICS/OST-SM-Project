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

### Prometheus Reporting for Anomalies

The inference service exposes detected anomalous sensor data as Prometheus metrics on port 8000. Each detected anomaly sets the `anomalous_sensor_event` gauge for the affected sensors.

#### How to use

1. Ensure the model container is running and port 8000 is accessible to Prometheus.
2. Add the following scrape config to your Prometheus configuration:

    ```yaml
        - job_name: 'model-anomaly-inference'
            static_configs:
                - targets: ['model:8000']  # Replace 'model' with the actual service name or IP
    ```

3. The metric `anomalous_sensor_event{sensor="SENSOR_NAME"}` will be set to the sensor value when an anomaly is detected, or 0 otherwise.

### Grafana Dashboard

A ready-to-import Grafana dashboard is provided at:
`deployment/grafana/provisioning/dashboards/anomalous_sensor_events.json`

This dashboard visualizes anomalous sensor events over time for all monitored sensors, making it easy for operators to interpret and respond to detected anomalies.
