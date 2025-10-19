# AutEncoder Model

This subproject contains files related to the AutoEncoder model.

## How to run the code

### Create the python virtual environment

First, the python virtual environment needs to be created with the all of the necessary dependencies installed.

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Run the app

Export the correct Java version before starting the Spark apps if the default one is incorrect.

```bash
export JAVA_HOME=`/usr/libexec/java_home -v 17`
```

#### Train

```bash
spark-submit train.py
```

#### Inference

```bash
spark-submit inference.py
```
