FROM pytorch/pytorch:2.9.0-cuda13.0-cudnn9-runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN java -version && python3 --version && pip3 --version

# Copy csv datasets
COPY datasets/ datasets/

# Copy requirements and install
COPY model/requirements.txt .
RUN pip install --no-cache-dir --break-system-packages -r requirements.txt

COPY model/weights/ model/weights/

# Copy your PySpark app
ADD model/*.py .

ENV PYTHONUNBUFFERED=1

# Set entrypoint
ENTRYPOINT ["python3"]
