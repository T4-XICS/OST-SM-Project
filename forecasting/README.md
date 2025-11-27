# Forecasting Module

## Overview

The forecasting module provides real-time time-series forecasting capabilities for Industrial Control Systems (ICS) sensor data. 
It processes streaming data from Kafka, generates forecasts using an LSTM-VAE model, and writes predictions to InfluxDB for monitoring and analysis.

## Architecture

The system consists of three main components:

1. **Spark Streaming Pipeline** - Consumes sensor data from Kafka in real-time
2. **LSTM-VAE Model** - Generates forecasts based on historical sequences
3. **InfluxDB Integration** - Stores forecasts for monitoring and analysis

## Model Details

### LSTM-VAE Architecture

The forecasting model uses an LSTM-based Variational Autoencoder with the following specifications:

- **Input Features**: 51 sensor features from the SWaT dataset
- **Sequence Length**: 10 timesteps
- **Forecast Horizon**: 10 timesteps ahead
- **Device**: CUDA-enabled GPU when available, CPU fallback

### Supported Features

The model processes 51 features across 6 process stages:

**Stage 1**: FIT101, LIT101, MV101, P101, P102  
**Stage 2**: AIT201, AIT202, AIT203, FIT201, MV201, P201-P206  
**Stage 3**: DPIT301, FIT301, LIT301, MV301-MV304, P301, P302  
**Stage 4**: AIT401, AIT402, FIT401, LIT401, P401-P404, UV401  
**Stage 5**: AIT501-AIT504, FIT501-FIT504, P501, P502, PIT501-PIT503  
**Stage 6**: FIT601, P601-P603

### Output Features

By default, forecasts are generated for selected features:
- **AIT201** - Analog Input Temperature (Stage 2)
- **FIT301** - Flow Input Temperature (Stage 3)
- **LIT101** - Level Input Temperature (Stage 1)

## Components

### `forecasting_stream.py`

Main streaming application that:
- Connects to Kafka topic `ics-sensor-data`
- Processes micro-batches of sensor readings
- Maintains sliding windows of sequences
- Triggers model inference
- Writes forecasts to InfluxDB

### `model_inference.py`

Model management and inference:
- Safe model loading with checkpointing
- Background thread for model hot-reloading (60s interval)
- Thread-safe model access
- Device-aware inference (GPU/CPU)

### `lstm_forecaster.py`

LSTM forecasting model implementation:
- Multi-layer LSTM architecture
- Configurable hidden dimensions and layers
- Multi-step ahead forecasting

### `network.py`

LSTM-VAE model implementation with encoder-decoder architecture and latent space representation.

## Configuration

### Environment Variables

```bash
# InfluxDB Configuration
INFLUX_URL=http://influxdb:8086
INFLUX_BUCKET=forecasting
INFLUX_ORG=ucs
INFLUX_TOKEN=<your_token>

# Model Configuration
MODEL_PATH=/weights/lstm_vae_swat.pth

# Model Parameters
INPUT_DIM = 51           # Number of sensor features
SEQ_LEN = 10            # Input sequence length
FORECAST_HORIZON = 10   # Forecast timesteps ahead
HIDDEN_DIM = 64         # LSTM hidden dimension
NUM_LAYERS = 2          # LSTM layers
```

### Deployment

```bash
# Docker Build
docker build -t forecasting:latest -f forecasting/Dockerfile .
```

### Dependencies
- torch==2.0.1
- numpy==1.26.4
- pandas==2.3.3
- pyspark==4.0.1
- kafka-python==2.0.2
- influxdb-client==1.38.0

### Additional JARs (auto-downloaded in Dockerfile):
- spark-sql-kafka-0-10_2.12-3.5.1.jar
- spark-token-provider-kafka-0-10_2.12-3.5.1.jar
- kafka-clients-3.5.1.jar
- commons-pool2-2.11.1.jar

## Troubleshooting

### Model Loading Failures

```bash
# Check model file exists
ls -lh /weights/lstm_vae_swat.pth

# Verify checkpoint format
python -c "import torch; print(torch.load('/weights/lstm_vae_swat.pth').keys())"
```

### Kafka Connection Issues

```bash
# Test Kafka connectivity
kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic ics-sensor-data --from-beginning
```
### InfluxDB Write Failures

```bash
# Verify InfluxDB connection
curl -I ${INFLUX_URL}/health

# Check bucket exists
influx bucket list --org ${INFLUX_ORG} --token ${INFLUX_TOKEN}
```

### Insufficient Sequence Length
```log
[INFO] Not enough rows for a sequence — skipping batch.
```
**Souliton**: 
Wait for more data to accumulate or reduce **SEQ_LEN**.
