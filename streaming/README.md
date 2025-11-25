# Event Pattern Mining Service  
(Streaming ICS Rule-Based Pattern Detector)

This service performs **streaming event-pattern mining** for Industrial Control System (ICS) data.  
It detects short multi-sensor patterns such as:

> **“FIT101 drops below a threshold, then within 60s LIT101 rises above a threshold.”**  
> → Pattern ID: `FIT_drop_then_LIT_rise`

Detected events are pushed to **InfluxDB** (`measurement = pattern_events`) or printed to stdout when Influx is disabled.

The implementation is contained in:  
`event_pattern_mining.py`  <!-- :contentReference[oaicite:0]{index=0} -->

---

##  Features

- Supports **Kafka streaming mode** and **CSV replay mode**  
- Rule-based pattern detection with cooldown and sliding window analysis  
- Optional **auto-tuning of thresholds** via quantiles using a normal SWaT dataset  
- Writes structured pattern events to **InfluxDB**  
- Fully configurable thresholds via environment variables  
- Detects short temporal causal patterns between sensors (e.g., FIT→LIT)

---

##  Installation

Install dependencies:

```bash
pip install pandas influxdb-client python-dotenv kafka-python
```

## Running the code

```bash
export INFLUX_URL=http://influxdb:8086
export INFLUX_TOKEN=your-token
export INFLUX_ORG=ucs
export INFLUX_BUCKET=swat_db

python event_pattern_mining.py \
    --test-file datasets/swat/normal/SWaT_Dataset_Normal_v0_1.csv
```
