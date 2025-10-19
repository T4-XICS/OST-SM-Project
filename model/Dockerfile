FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.12 python3-pip openjdk-17-jdk ca-certificates && \
    ln -sf /usr/bin/python3.12 /usr/bin/python3 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Verify
RUN java -version && python3 --version && pip3 --version

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir --break-system-packages -r requirements.txt

# Copy your PySpark app
ADD *.py .

# Set entrypoint
ENTRYPOINT ["python3", "inference.py"]

