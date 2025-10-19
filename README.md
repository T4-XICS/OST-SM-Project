# X-ICS-IncreADStream: Explainable Incremental Autoencoder-Based Real-Time Anomaly Detection for Industrial Control Systems

## [Overleaf](https://www.overleaf.com/read/bxfwgjnkvfgj#466549)

## Setting up Git LFS

Datasets are stored in Git Large File Storage. To set it up download Git LFS from [git-lfs.com](https://git-lfs.com/) or set it up using the `git lfs install` command. This should be done before cloning for all of the files to properly download but can be done after cloning as well by using `git lfs fetch` and `git lfs checkout`.

## Description

This project will build a system to find and explain unusual behavior (anomalies) in real-time data from Industrial Control Systems (ICS). These systems are used in places like water treatment plants, power stations, or factories. The system will use a deep learning model (an autoencoder) to learn how the system normally works and detect when something strange happens.
The system will also learn continuously over time (called incremental learning). This helps it adapt to changes in the system, such as slow wear and tear or different working conditions. When it finds an anomaly, the system will explain which sensors caused it, so engineers can fix the problem faster.
The system will work with any streaming data tool (like Kafka,or others). It will also include a live dashboard to show data and send alerts when needed.
