#!/usr/bin/env python3
"""
Prometheus to InfluxDB exporter that scrapes Prometheus metrics and writes to InfluxDB.
Continuously polls Prometheus and stores metrics in InfluxDB for long-term storage.

Dependencies: prometheus-api-client, influxdb-client

Usage:
pip install prometheus-api-client influxdb-client
python consumer/prometheus_to_influx.py --prometheus-url http://localhost:9090 \
  --influx-url http://localhost:8086 --influx-org ucs --influx-bucket prometheus --token <TOKEN>
"""
import argparse
import time
from datetime import datetime
from prometheus_api_client import PrometheusConnect
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

def fetch_and_write_metrics(prom_client, write_api, influx_bucket, influx_org, queries):
    """
    Fetch metrics from Prometheus and write to InfluxDB.
    
    Args:
        prom_client: PrometheusConnect instance
        write_api: InfluxDB write API
        influx_bucket: InfluxDB bucket name
        influx_org: InfluxDB organization
        queries: List of PromQL queries to execute
    """
    points = []
    timestamp = datetime.utcnow()
    
    for query in queries:
        try:
            result = prom_client.custom_query(query=query)
            
            if not result:
                print(f"No data returned for query: {query}")
                continue
            
            for metric in result:
                # Extract metric name and labels
                metric_name = metric['metric'].get('__name__', 'unknown')
                labels = {k: v for k, v in metric['metric'].items() if k != '__name__'}
                
                # Get the value
                value = float(metric['value'][1])
                
                # Create InfluxDB point
                point = Point(metric_name)
                
                # Add labels as tags
                for label_key, label_value in labels.items():
                    point.tag(label_key, label_value)
                
                # Add value as field
                point.field("value", value)
                point.time(timestamp, WritePrecision.S)
                
                points.append(point)
                
        except Exception as e:
            print(f"Error processing query '{query}': {e}")
    
    # Write all points to InfluxDB
    if points:
        try:
            write_api.write(bucket=influx_bucket, org=influx_org, record=points)
            print(f"✓ Wrote {len(points)} metrics to InfluxDB at {timestamp}")
        except Exception as e:
            print(f"✗ Error writing to InfluxDB: {e}")
    else:
        print(f"No metrics collected at {timestamp}")

def main():
    p = argparse.ArgumentParser(description="Export Prometheus metrics to InfluxDB")
    p.add_argument("--prometheus-url", default="http://localhost:9090", 
                   help="Prometheus server URL")
    p.add_argument("--influx-url", default="http://localhost:8086",
                   help="InfluxDB server URL")
    p.add_argument("--influx-org", default="ucs",
                   help="InfluxDB organization")
    p.add_argument("--influx-bucket", default="prometheus",
                   help="InfluxDB bucket name")
    p.add_argument("--token", required=True,
                   help="InfluxDB authentication token")
    p.add_argument("--interval", type=int, default=15,
                   help="Scrape interval in seconds (default: 15)")
    p.add_argument("--queries", nargs='+', 
                   help="Custom PromQL queries to execute (space-separated)")
    args = p.parse_args()

    print(f"Connecting to Prometheus at {args.prometheus_url}...")
    print(f"Writing to InfluxDB at {args.influx_url} (bucket: {args.influx_bucket})")
    print(f"Scrape interval: {args.interval} seconds")
    print()

    # Create Prometheus client
    prom = PrometheusConnect(url=args.prometheus_url, disable_ssl=True)
    
    # Test Prometheus connection
    try:
        prom.check_prometheus_connection()
        print("✓ Successfully connected to Prometheus")
    except Exception as e:
        print(f"✗ Failed to connect to Prometheus: {e}")
        raise

    # Create InfluxDB client
    print(f"Connecting to InfluxDB at {args.influx_url}...")
    influx_client = InfluxDBClient(url=args.influx_url, token=args.token, org=args.influx_org)
    
    # Test InfluxDB connection
    try:
        health = influx_client.ping()
        print(f"✓ InfluxDB connection successful: {health}")
    except Exception as e:
        print(f"✗ InfluxDB connection failed: {e}")
        raise
        
    write_api = influx_client.write_api(write_options=SYNCHRONOUS)
    print("✓ InfluxDB write API initialized")
    print()

    # Define default queries if none provided
    if args.queries:
        queries = args.queries
    else:
        # Default queries - fetch all metrics from the main jobs
        queries = [
            'up',  # Service availability
            '{job="kafka-producer"}',  # All metrics from kafka-producer
            '{job="prometheus"}',  # All metrics from prometheus itself
            'go_memstats_alloc_bytes',  # Memory usage
            'process_cpu_seconds_total',  # CPU usage
            'prometheus_http_requests_total',  # HTTP requests
        ]
    
    print("Queries to execute:")
    for query in queries:
        print(f"  - {query}")
    print()

    try:
        print("Starting metric collection loop...")
        iteration = 0
        while True:
            iteration += 1
            print(f"\n--- Iteration {iteration} at {datetime.now()} ---")
            fetch_and_write_metrics(prom, write_api, args.influx_bucket, args.influx_org, queries)
            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\n\nStopping exporter...")
    finally:
        influx_client.close()
        print("InfluxDB client closed. Exiting.")

if __name__ == "__main__":
    main()
