FROM python:3.10-slim

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Create working directories
WORKDIR /app
RUN mkdir -p /app/feature_engineering /app/datasets /app/feature_selection_results

COPY feature_engineering/ /app/feature_engineering/

# Install dependencies from the folder
RUN pip install --no-cache-dir -r /app/feature_engineering/requirements.txt

# Ensure Python can import this folder
ENV PYTHONPATH="/app/feature_engineering:${PYTHONPATH}"

WORKDIR /app/feature_engineering

# Default command (can be overridden)
CMD ["python", "feature_selection.py"]
