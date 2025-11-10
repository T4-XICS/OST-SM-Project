# X-ICS-IncreADStream: Explainable Incremental Autoencoder-Based Real-Time Anomaly Detection for Industrial Control Systems

## [Overleaf](https://www.overleaf.com/read/bxfwgjnkvfgj#466549)

## Setting up Git LFS

Datasets are stored in Git Large File Storage. To set it up download Git LFS from [git-lfs.com](https://git-lfs.com/) or set it up using the `git lfs install` command. This should be done before cloning for all of the files to properly download but can be done after cloning as well by using `git lfs fetch` and `git lfs checkout`.

## Description

This project will build a system to find and explain unusual behavior (anomalies) in real-time data from Industrial Control Systems (ICS). These systems are used in places like water treatment plants, power stations, or factories. The system will use a deep learning model (an autoencoder) to learn how the system normally works and detect when something strange happens.
The system will also learn continuously over time (called incremental learning). This helps it adapt to changes in the system, such as slow wear and tear or different working conditions. When it finds an anomaly, the system will explain which sensors caused it, so engineers can fix the problem faster.
The system will work with any streaming data tool (like Kafka,or others). It will also include a live dashboard to show data and send alerts when needed.

## SWaT sensor & actuator reference

> **Notes:** Timestamp is the logged time (1 Hz sampling). `Normal_Attack` is a label column indicating whether the row is from normal operation or an attack scenario.

---

### Stage P1 — Raw water intake / storage

| Tag    | Type                                    | Description                                              | Unit   |
|--------|-----------------------------------------|----------------------------------------------------------|--------|
| FIT101 | Flow indicator transmitter (FIT)        | Inlet / raw-water flow rate into the raw-water tank      | L/min  |
| LIT101 | Level indicator transmitter (LIT)       | Raw-water tank level                                     | cm     |
| MV101  | Motorized valve (MV)                    | Valve controlling raw-water inflow (open/closed)         | binary |
| P101   | Pump (P)                                | Raw-water feed pump (on/off)                             | binary |
| P102   | Pump (P)                                | Redundant/raw-water feed pump (on/off)                   | binary |

---

### Stage P2 — Pre-treatment / chemical dosing

| Tag         | Type                                    | Description                                                      | Unit    |
|-------------|-----------------------------------------|------------------------------------------------------------------|---------|
| AIT201      | Analyzer indicator transmitter (AIT)    | pH / chemical analyzer for pre-treatment dosing control         | pH      |
| AIT202      | Analyzer indicator transmitter (AIT)    | ORP / redox potential for dosing control                        | mV      |
| AIT203      | Analyzer indicator transmitter (AIT)    | Conductivity / water quality after dosing                        | µS/cm   |
| FIT201      | Flow indicator transmitter (FIT)        | Flow rate into pre-treatment / dosing unit                       | L/min   |
| MV201       | Motorized valve (MV)                    | Valve controlling flow into dosing unit                          | binary  |
| P201–P206   | Pumps (P)                               | Chemical dosing pumps / small process pumps (on/off)             | binary  |

---

### Stage P3 — Ultrafiltration (UF)

| Tag           | Type                                         | Description                                                    | Unit   |
|---------------|----------------------------------------------|----------------------------------------------------------------|--------|
| DPIT301       | Differential pressure indicator transmitter  | Pressure drop across UF membranes (fouling indicator)          | bar    |
| FIT301        | Flow indicator transmitter (FIT)             | Flow through the UF module                                     | L/min  |
| LIT301        | Level indicator transmitter (LIT)            | UF feed / buffer tank level                                    | cm     |
| MV301–MV304   | Motorized valves (MV)                        | Valves controlling UF feed / permeate / reject lines           | binary |
| P301, P302    | Pumps (P)                                    | UF feed / backwash pumps (on/off)                              | binary |

---

### Stage P4 — Dechlorination / holding

| Tag         | Type                                 | Description                                           | Unit                    |
|-------------|--------------------------------------|-------------------------------------------------------|-------------------------|
| AIT401      | Analyzer indicator transmitter (AIT) | Chlorine / residual disinfectant analyzer            | ppm (or relative unit)  |
| AIT402      | Analyzer indicator transmitter (AIT) | ORP / related chemical quality measure               | mV                      |
| FIT401      | Flow indicator transmitter (FIT)     | Flow into dechlorination / holding stage             | L/min                   |
| LIT401      | Level indicator transmitter (LIT)    | Dechlorination / holding tank level                  | cm                      |
| P401–P404   | Pumps (P)                            | Pumps for the dechlorination / holding stage (on/off)| binary                  |
| UV401       | UV disinfection unit                 | UV lamp / disinfection device (on/off)               | binary                  |

---

### Stage P5 — Reverse Osmosis (RO)

| Tag               | Type                                    | Description                                                           | Unit                        |
|-------------------|-----------------------------------------|-----------------------------------------------------------------------|-----------------------------|
| AIT501–AIT504     | Analyzer indicator transmitters (AIT)   | Conductivity / pH / quality sensors across RO feed, permeate, reject  | µS/cm, pH (stage-dependent) |
| FIT501–FIT504     | Flow indicator transmitters (FIT)       | Flows for RO feed, permeate and reject streams                        | L/min                       |
| P501, P502        | Pumps (P)                               | High-pressure RO pumps (on/off)                                       | binary                      |
| PIT501–PIT503     | Pressure indicator transmitters (PIT)   | Pressures at RO feed / permeate / reject points                       | bar                         |

---

### Stage P6 — Product water / distribution

| Tag        | Type                     | Description                                    | Unit   |
|------------|--------------------------|------------------------------------------------|--------|
| FIT601     | Flow indicator transmitter (FIT) | Final product flow to storage / distribution | L/min  |
| P601–P603  | Pumps (P)                | Product / storage pumps for final distribution (on/off) | binary |

---

## Quick Start — Local demo stack & end-to-end usage

This section explains how to run the local development stack and exercise the pipeline (CSV → Kafka → PySpark → InfluxDB → Grafana/Prometheus). It assumes Docker (Docker Desktop) is installed and working and that you have cloned the repository.

### Repo layout (important files)

- `deployment/` — docker-compose and provisioning (Kafka, Zookeeper, InfluxDB, Grafana, Prometheus).  
- `datasets/swat/` — put SWaT (or other) sample CSVs here.
- `model/` — place trained model checkpoints (e.g., `checkpoint.pt`) here.  

---

### 0) Prerequisites

- Docker Desktop (Windows/macOS) or Docker Engine (Linux).  
- Python 3.8+ for producer scripts.  
- (Optional) Apache Spark if you run `pyspark_consumer.py` via `spark-submit` locally.

---

### 1) Start the stack

Run from the `deployment/` folder:

```bash
cd deployment
docker compose up -d --build
```

### 2) What runs where (default URLs & credentials)

- Grafana (UI): `http://localhost:3000`
  - Username: `admin`
  - Password: `admin2`
- InfluxDB (UI): `http://localhost:8086`
  - Username: `admin`
  - Password: `adminpass`
  - Default bucket: `swat_db`
  - Default org: `ucs`
  - Admin token: Set via `INFLUXDB_TOKEN` environment variable (e.g., `admintoken123`)
- Prometheus (UI): `http://localhost:9090`
- Kafka (UI): `http://localhost:9092`

#### Local Environment Variables (.env)

To run the project locally, you need to provide an InfluxDB token as an environment variable. This is done by creating a `.env` file in the `deployment/` directory (the same directory as the `docker-compose.yml` file). Docker Compose will automatically load environment variables from this file.

**Steps:**

1. In the `deployment/` directory, create a file named `.env` (if it does not already exist).
2. Add the following line to the `.env` file:

  ```env
  INFLUXDB_TOKEN=admintoken123
  ```

### In case of something is not working

- If one or more of the applications are not working correctly, for example showing something else than to the others, you should head to the Docker Desktop - Volumes and delete the problematic application volume.
- There might be some leftover information in the volumes saved, and a `docker compose up -d --build` will not rebuild it properly
