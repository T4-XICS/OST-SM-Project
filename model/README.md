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
