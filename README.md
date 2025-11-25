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


# Literature Review

## A Review on Outlier/Anomaly Detection in Time Series Data
This work provides a review of outlier and anomaly detection through a new taxonomy in the time-series context. It analyses techniques based on input data type, outlier type, and characteristics of the method. It defines three distinct outlier categories: point outliers, subsequence outliers, and outlier time series. Methods are categorized as univariate or multivariate and evaluated for applicability to streaming data—i.e., whether they can detect anomalies using only past and present information. The paper also lists publicly available software related to the described techniques.  
**Reference:** Blázquez-García et al. (2020)

---

## Bidirectional LSTM Autoencoder for Sequence-Based Anomaly Detection in Cyber Security
This paper proposes a host-based intrusion detection system using a Bidirectional LSTM Encoder and a unidirectional Decoder trained exclusively on normal ADFA-LD system-call sequences. Anomalies are detected using low reconstruction probability. A major contribution is the use of CuDNNLSTM, which reduces training time by a factor of 10. Models achieve up to 0.86 AUC and ~90% detection rate with a 25% false-alarm rate.  
**References:** Chawla et al. (2019), Creech & Hu (2013)

---

## StackVAE-G: Efficient and Interpretable Time-Series Anomaly Detection
StackVAE-G integrates a variational autoencoder (VAE) with a graph neural network (GNN) to capture both temporal and spatial relationships between sensors. A sparse adjacency matrix is learned to model stable inter-sensor relations. The method improves accuracy and interpretability while reducing computational overhead. Tests on SMD, SMAP, and MSL show strong performance with low resource usage.  
**Reference:** Li et al. (2022)

---

## A Machine Learning Approach for Anomaly Detection in Industrial Control Systems
This work uses measurement-level ICS data—sensor readings and logs—to detect cyberattacks and process anomalies. It emphasizes domain-driven preprocessing (statistical, windowed, spectral features) and compares several ML models such as decision trees, random forests, and neural networks. It discusses deployment challenges like timestamp alignment and label uncertainty.  
**Reference:** Mokhtari et al. (2021)

---

## ICS-Flow: An Anomaly Detection Dataset for Industrial Control Systems
ICS-Flow provides synchronized network flows, packet captures, and PLC process states collected from a bottle-filling testbed. The dataset includes labeled cyberattacks such as reconnaissance, MitM, replay, and DDoS. The paper also introduces ICSFlowGenerator for feature extraction from pcaps. Combined network + process features improve detection coverage.  
**Reference:** Dehlaghi-Ghadim et al. (2023)

---

## SUSAN: A Deep-Learning Framework for Sustainable Anomaly Detection
SUSAN augments anomaly detection with sustainability metrics (e.g., energy impact). It uses engineered features and multiple deep-learning models (LSTM, autoencoders, CNNs). Results show sustainability-aware scoring helps prioritize high-impact anomalies. The framework includes lightweight variants for edge deployment.  
**Reference:** Gómez et al. (2023)

---

## LLM-Based Detection of Cyber Anomalies in Industrial Control Systems
This paper applies BERT to ICS anomaly detection using tokenized system logs. It outperforms LSTM and TF-IDF + SVM baselines on SWaT and Morris datasets (F1 = 0.825, accuracy = 90.5%). Strengths include contextual learning and reduced need for manual feature engineering. Limitations include reliance on labeled data and absence of live deployment tests.  
**Reference:** Thali & Pachghare (2024)

---

## Enhanced Anomaly Detection in ICS via Machine Learning
This study combines network traffic features (from Zeek) with process-level data from the SWaT dataset. Multiple ML models—Random Forest, SVM, kNN, neural networks—are trained on three configurations: network-only, process-only, and hybrid. The hybrid approach yields the best recall and overall performance.  
**Reference:** Berge & Li et al. (2024)

---

## Multi-Level Anomaly Detection in ICS using Package Signatures and LSTM
A two-tier framework combining Bloom-filter-based packet signature detection with a stacked LSTM for temporal anomaly detection. Achieves F1 = 0.85 on a gas pipeline SCADA dataset. Advantages include zero-day attack detection and low computational overhead.  
**Reference:** Feng et al. (2017)

---

## Unsupervised Learning Approach for Anomaly Detection in ICS
This work trains a k-means clustering model to detect anomalies without labeled attack data. Statistical features (min, max, mean, std) are extracted from time windows and used for unsupervised modeling. Tested on SWaT, results show unsupervised detection is feasible with careful feature design.  
**Reference:** Choi et al. (2024)

---

## One-Class SVM for Anomaly Detection
One-class SVM is used to identify abnormal transactions without having any fraud labels during training. It performs more consistently than traditional binary classifiers on imbalanced datasets and avoids overfitting.  
**Reference:** Hejazi et al. (2013)

---

## Anomaly Detection in ICS with LSTM Autoencoders
This paper uses an LSTM autoencoder trained on normal operation to detect anomalies via reconstruction + prediction error. Errors are smoothed with EWMA to reduce noise, and a change-ratio mechanism aids root-cause identification. Evaluated on the SWaT dataset.  
**Reference:** Wang et al. (2020)





# References

**Goh et al. (2017)**  
A Dataset to Support Research in the Design of Secure Water Treatment Systems.  
*Critical Information Infrastructures Security*.  
https://doi.org/10.1007/978-3-319-71368-7_8

**Mokhtari et al. (2021)**  
A machine learning approach for anomaly detection in industrial control systems based on measurement data.  
*Electronics*, 10(4).  
https://doi.org/10.3390/electronics10040407

**Blázquez-García et al. (2020)**  
A review on outlier/anomaly detection in time series data.  
arXiv:2002.04236  
https://arxiv.org/abs/2002.04236

**Dehlaghi-Ghadim et al. (2023)**  
Anomaly Detection Dataset for Industrial Control Systems.  
*IEEE Access*, 11.  
https://doi.org/10.1109/ACCESS.2023.3320928

**Wang et al. (2020)**  
Anomaly Detection for Industrial Control System Based on Autoencoder Neural Network.  
*Wireless Communications and Mobile Computing*, 2020.  
https://doi.org/10.1155/2020/8897926

**Giordano et al. (2021)**  
Anomaly detection in the CERN cloud infrastructure.  
*EPJ Web of Conferences*, 251:02011.  
https://doi.org/10.1051/epjconf/202125102011

**Li et al. (2023)**  
Autoencoder-based Anomaly Detection in Streaming Data with Incremental Learning and Concept Drift Adaptation.  
arXiv:2305.08977  
https://arxiv.org/abs/2305.08977  
https://doi.org/10.1109/IJCNN54540.2023.10191328

**Chawla et al. (2019)**  
Bidirectional LSTM Autoencoder for Sequence Based Anomaly Detection in Cyber Security.  
*International Journal of Simulation: Systems, Science & Technology*.  
https://doi.org/10.5013/ijssst.a.20.05.07

**Abdelaty et al. (2020)**  
DAICS: A Deep Learning Solution for Anomaly Detection in Industrial Control Systems.  
arXiv:2009.06299  
https://arxiv.org/abs/2009.06299  
https://doi.org/10.1109/TETC.2021.3073017

**Berge & Li (2024)**  
Enhanced Anomaly Detection in Industrial Control Systems aided by Machine Learning.  
arXiv:2410.19717  
https://arxiv.org/abs/2410.19717

**Ha et al. (2022)**  
Explainable Anomaly Detection for Industrial Control System Cybersecurity.  
arXiv:2205.01930  
https://arxiv.org/abs/2205.01930

**Amer et al. (2024)**  
From Black Boxes to Transparent Insights: Enhancing ICS Anomaly Detection with Deep Autoencoder Models.  
*MIUCC 2024*, pp. 380–386.  
https://doi.org/10.1109/MIUCC62295.2024.10783503

**Creech & Hu (2013)**  
Generation of a new IDS test dataset: Time to retire the KDD collection.  
*IEEE WCNC 2013*, pp. 4487–4492.  
https://doi.org/10.1109/WCNC.2013.6555301

**Silva et al. (Year N/A)**  
Lecture Notes in Mechanical Engineering: Flexible Automation and Intelligent Manufacturing.  
https://link.springer.com/bookseries/11693

**Thali & Pachghare (Year N/A)**  
LLM-Based Detection of Cyber Anomalies in Industrial Control Systems.

**Feng et al. (2017)**  
Multi-level Anomaly Detection in Industrial Control Systems via Package Signatures and LSTM Networks.  
*IEEE/IFIP DSN 2017*, pp. 261–272.  
https://doi.org/10.1109/DSN.2017.34

**Hejazi & Singh (2013)**  
One-class support vector machines approach to anomaly detection.  
*Applied Artificial Intelligence*, 27(5), 351–366.  
https://doi.org/10.1080/08839514.2013.785791

**Akiba et al. (2019)**  
Optuna: A Next-generation Hyperparameter Optimization Framework.  
*KDD 2019*, pp. 2623–2631.  
https://doi.org/10.1145/3292500.3330701

**Li et al. (2022)**  
StackVAE-G: An efficient and interpretable model for time-series anomaly detection.  
*AI Open*, 3:101–110.  
https://doi.org/10.1016/j.aiopen.2022.07.001

**Gómez et al. (2023)**  
SUSAN: A Deep Learning based anomaly detection framework for sustainable industry.  
*Sustainable Computing: Informatics and Systems*, 37.  
https://doi.org/10.1016/j.suscom.2022.100842

**Choi & Kim (2024)**  
Unsupervised Learning Approach for Anomaly Detection in Industrial Control Systems.  
*Applied System Innovation*, 7(2).  
https://doi.org/10.3390/asi7020018
