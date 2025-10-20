FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.12 python3-pip openjdk-17-jdk ca-certificates && \
    ln -sf /usr/bin/python3.12 /usr/bin/python3 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Verify
RUN java -version && python3 --version && pip3 --version

# Copy csv datasets
COPY datasets/ datasets/

# Copy requirements and install
COPY model/requirements.txt .
RUN pip install --no-cache-dir --break-system-packages -r requirements.txt

COPY model/weights/ model/weights/

# Copy your PySpark app
ADD model/*.py .

# Set entrypoint
ENTRYPOINT ["python3"]
