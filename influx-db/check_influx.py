#!/usr/bin/env python3
"""
Simple script to check InfluxDB buckets and data.
Uses the token from your .env file.
"""
from influxdb_client import InfluxDBClient

# Configuration
INFLUX_ORG = "ucs"
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "admintoken123"  # From your .env file

def main():
    print("=" * 70)
    print("InfluxDB Status Check")
    print("=" * 70)
    print(f"URL: {INFLUX_URL}")
    print(f"Organization: {INFLUX_ORG}")
    print()

    try:
        # Create client
        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        
        # Check connection
        print("✓ Successfully connected to InfluxDB")
        
        # List all buckets
        buckets_api = client.buckets_api()
        buckets = buckets_api.find_buckets().buckets
        
        print(f"\n📦 Found {len(buckets)} buckets:")
        print("-" * 70)
        for bucket in buckets:
            print(f"  • {bucket.name} (ID: {bucket.id})")
        
        # Check each bucket for data
        query_api = client.query_api()
        
        print("\n📊 Data Summary:")
        print("-" * 70)
        
        for bucket in buckets:
            if bucket.name.startswith('_'):  # Skip system buckets
                continue
                
            # Count measurements in each bucket
            query = f'''
            import "influxdata/influxdb/schema"
            schema.measurements(bucket: "{bucket.name}")
            '''
            
            try:
                tables = query_api.query(query, org=INFLUX_ORG)
                measurements = []
                for table in tables:
                    for record in table.records:
                        measurements.append(record.get_value())
                
                if measurements:
                    print(f"\n  Bucket: {bucket.name}")
                    print(f"  Measurements: {', '.join(measurements)}")
                    
                    # Get count for each measurement (SWaT data is from 2015!)
                    for measurement in measurements:
                        count_query = f'''
                        from(bucket: "{bucket.name}")
                          |> range(start: 2015-01-01T00:00:00Z)
                          |> filter(fn: (r) => r._measurement == "{measurement}")
                          |> count()
                        '''
                        try:
                            count_tables = query_api.query(count_query, org=INFLUX_ORG)
                            total = 0
                            for table in count_tables:
                                for record in table.records:
                                    total += record.get_value()
                            print(f"    - {measurement}: {total} records (last 30 days)")
                        except:
                            print(f"    - {measurement}: Unable to count")
                else:
                    print(f"\n  Bucket: {bucket.name}")
                    print(f"  Status: Empty (no measurements)")
                    
            except Exception as e:
                print(f"\n  Bucket: {bucket.name}")
                print(f"  Error: {str(e)}")
        
        print("\n" + "=" * 70)
        print("Check complete!")
        print("\n💡 Tip: Access the web UI at http://localhost:8086")
        print("   Login: admin / adminpass")
        
        client.close()
        
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        print("\nMake sure:")
        print("  1. InfluxDB container is running")
        print("  2. Token is correct (should be 'admintoken123' from .env)")
        print("  3. Port 8086 is accessible")

if __name__ == "__main__":
    main()
