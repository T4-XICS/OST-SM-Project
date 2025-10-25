# Kubernetes deployment for OST-SM-Project

## What’s included

- `namespace.yaml`: Creates namespace `ics` to isolate all resources.
- `kafka.yaml`: Single-node KRaft Kafka `Deployment` + `Service` + `PVC` (`kafka-data`).
- `influxdb.yaml`: InfluxDB `Deployment` + `Service` + `PVC` (`influxdb-data`) with identical setup env vars.
- `grafana-config.yaml`: ConfigMaps for Grafana provisioning (datasources and dashboards) from repo files.
- `grafana.yaml`: Grafana `Deployment` + `Service` + `PVC` (`grafana-data`) mounting provisioning ConfigMaps read-only.
- `prometheus-config.yaml`: ConfigMap embedding your `prometheus.yml`.
- `prometheus.yaml`: Prometheus `Deployment` + `Service` + `PVC` (`prometheus-data`).
- `producer.yaml`: Producer `Deployment` mounting a `PVC` at `/app` to provide the CSV file at the same path as docker-compose.
- `spark-consumer.yaml`: Spark + PyTorch consumer `Deployment` requesting 1 GPU and running `inference.py`.
- `kustomization.yaml`: For easy apply/delete of the whole stack.

## Images

The docker-compose file builds two images from source. For Kubernetes, build and push them to a registry you can pull from and update the image names in the manifests:

- Producer: build from `producer/` and bake the CSV into the image:
  - Build context must be repo root so the Dockerfile can copy `datasets/swat/normal/...`.
  - Example:
    ```
    docker build -f ./producer/Dockerfile -t <your_name>/producer:latest .
    docker push <your_name>/producer:latest
    ```
  - `producer.yaml` uses this image and no longer mounts a PVC.

- Spark consumer: build from `model/` → set `image: <your_name>/model:latest` in `spark-consumer.yaml`.

## Start the stack

```bash
# Apply everything
kubectl apply -k ./deployment/k8s

# (Optional) Watch pods
kubectl -n ics get pods -w
```

Services exposed as ClusterIP by default:
- Kafka: `kafka.ics.svc.cluster.local:9092`
- InfluxDB: `influxdb.ics.svc.cluster.local:8086`
- Grafana: `grafana.ics.svc.cluster.local:3000`
- Prometheus: `prometheus.ics.svc.cluster.local:9090`

To access Grafana and Prometheus from your machine, use port-forwarding:

```bash
kubectl -n ics port-forward svc/grafana 3000:3000
kubectl -n ics port-forward svc/prometheus 9090:9090
```

## Notes

- The `spark-consumer` requests `nvidia.com/gpu: 1`. Ensure the NVIDIA device plugin is installed on your cluster and a GPU node is available.
- Init containers in `producer` and `spark-consumer` wait for `kafka:9092` to be reachable before starting.
- The Kafka `Service` name is `kafka` to match the bootstrap server URL used in code/config (`kafka:9092`).
- Prometheus config currently contains a placeholder static target for Kafka. Point it to real `/metrics` endpoints as they are added.
