# CSV → Kafka Producers (SWaT ICS Streams)

This folder contains two Python scripts that publish **SWaT ICS data** from CSV files into **Kafka topics**. One script streams **normal** process data with Prometheus monitoring, and the other replays **attack** traces.

---

## Files

- `send_csv_to_kafka.py`  
  Streams the **SWaT Normal dataset** into Kafka and exposes **Prometheus metrics** on port `8000`.

- `producer.py`  
  Simple producer that streams the **SWaT Attack dataset** into a separate Kafka topic (no Prometheus).

---

## Prerequisites

- Python 3.9+  
- Kafka broker running and reachable
- SWaT CSV datasets available at the expected paths:
  - Normal: `datasets/swat/normal/SWaT_Dataset_Normal_v0_1.csv`
  - Attack: `datasets/swat/attack/SWaT_Dataset_Attack_v0_1.csv`
- Python packages:
  ```bash
  pip install kafka-python prometheus-client
