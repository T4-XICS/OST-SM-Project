#!/usr/bin/env python3
"""
Query the 'prometheus' bucket in InfluxDB to see Prometheus metrics stored there.
"""
from influxdb_client import InfluxDBClient
import pandas as pd

INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "admintoken123"
INFLUX_ORG = "ucs"
INFLUX_BUCKET = "prometheus"

def query_prometheus_bucket(time_range="-1h", limit=100):
    """Query the prometheus bucket in InfluxDB"""
    
    print("=" * 70)
    print("PROMETHEUS BUCKET QUERY (in InfluxDB)")
    print("=" * 70)
    print(f"URL: {INFLUX_URL}")
    print(f"Bucket: {INFLUX_BUCKET}")
    print(f"Time range: {time_range}")
    print()
    
    try:
        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        query_api = client.query_api()
        
        # First, check what measurements exist
        print("Checking measurements in prometheus bucket...")
        measurements_query = f'''
        import "influxdata/influxdb/schema"
        schema.measurements(bucket: "{INFLUX_BUCKET}")
        '''
        
        try:
            tables = query_api.query(measurements_query, org=INFLUX_ORG)
            measurements = []
            for table in tables:
                for record in table.records:
                    measurements.append(record.get_value())
            
            if measurements:
                print(f"✅ Found {len(measurements)} measurements:")
                for m in measurements[:20]:
                    print(f"  - {m}")
                if len(measurements) > 20:
                    print(f"  ... and {len(measurements) - 20} more")
            else:
                print("⚠️  No measurements found in prometheus bucket")
                print("   This means prometheus-to-influx hasn't written any data yet")
                print("\nTo debug:")
                print("  1. Check 'prometheus-to-influx' container logs")
                print("  2. Make sure the container is running")
                print("  3. Verify Prometheus has metrics to scrape")
                client.close()
                return
                
        except Exception as e:
            print(f"⚠️  Cannot list measurements: {e}")
            print("   The bucket might be empty")
            client.close()
            return
        
        # Query actual data
        print(f"\n" + "=" * 70)
        print("SAMPLE DATA")
        print("=" * 70)
        
        # Get some recent data
        data_query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: {time_range})
          |> limit(n: {limit})
        '''
        
        tables = query_api.query(data_query, org=INFLUX_ORG)
        
        results = []
        for table in tables:
            for record in table.records:
                results.append({
                    'time': record.get_time(),
                    'measurement': record.get_measurement(),
                    'field': record.get_field(),
                    'value': record.get_value(),
                    'tags': {k: v for k, v in record.values.items() 
                            if k not in ['_time', '_measurement', '_field', '_value', 
                                        'result', 'table', '_start', '_stop']}
                })
        
        if results:
            df = pd.DataFrame(results)
            print(f"\n✅ Found {len(results)} data points")
            print(f"\nFirst 10 records:")
            print(df.head(10).to_string())
            
            print(f"\n" + "=" * 70)
            print("DATA SUMMARY")
            print("=" * 70)
            print(f"Time range: {df['time'].min()} to {df['time'].max()}")
            print(f"\nMeasurements found:")
            print(df['measurement'].value_counts())
            
            # Count by measurement
            print(f"\nTotal records per measurement:")
            counts_query = f'''
            from(bucket: "{INFLUX_BUCKET}")
              |> range(start: {time_range})
              |> group(columns: ["_measurement"])
              |> count()
            '''
            
            count_tables = query_api.query(counts_query, org=INFLUX_ORG)
            for table in count_tables:
                for record in table.records:
                    measurement = record.values.get('_measurement', 'unknown')
                    count = record.get_value()
                    print(f"  - {measurement}: {count} records")
        else:
            print("⚠️  No data found in the specified time range")
            print(f"   Tried querying with: {time_range}")
            print("\nTry a larger time range, e.g.:")
            print("  - '-24h' for last 24 hours")
            print("  - '-7d' for last 7 days")
            print("  - '2015-01-01T00:00:00Z' for all data from 2015")
        
        client.close()
        
    except Exception as e:
        print(f"Error: {e}")
        print("\nMake sure:")
        print("  1. InfluxDB is running")
        print("  2. Token is correct")
        print("  3. prometheus bucket exists")

def main():
    # Try different time ranges
    time_ranges = ["-1h", "-24h", "-7d", "2015-01-01T00:00:00Z"]
    
    print("Trying to find data with different time ranges...\n")
    
    for time_range in time_ranges:
        print(f"\n{'='*70}")
        print(f"Querying with time range: {time_range}")
        print('='*70)
        query_prometheus_bucket(time_range=time_range, limit=10)
        
        # If we find data, stop trying other ranges
        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        query_api = client.query_api()
        test_query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: {time_range})
          |> limit(n: 1)
        '''
        tables = query_api.query(test_query, org=INFLUX_ORG)
        has_data = False
        for table in tables:
            for record in table.records:
                has_data = True
                break
            if has_data:
                break
        client.close()
        
        if has_data:
            print("\n✅ Found data! Stopping search.")
            break
        else:
            print(f"\n⚠️  No data in range {time_range}, trying next range...")

if __name__ == "__main__":
    main()
