#!/usr/bin/env python3
"""
Simple InfluxDB query to show attack data exists
"""
from influxdb_client import InfluxDBClient

INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "admintoken123"
INFLUX_ORG = "ucs"
INFLUX_BUCKET = "swat_db"

def main():
    print("=" * 80)
    print("INFLUXDB ATTACK DATA QUERY")
    print("=" * 80)
    
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    query_api = client.query_api()
    
    # Query 1: Count by status
    print("\n📊 QUERY 1: Count Records by Status")
    print("-" * 80)
    
    query = '''
    from(bucket: "swat_db")
      |> range(start: 2015-01-01T00:00:00Z)
      |> group(columns: ["status"])
      |> count()
    '''
    
    tables = query_api.query(query, org=INFLUX_ORG)
    status_counts = {}
    for table in tables:
        for record in table.records:
            status = record.values.get('status', 'Unknown')
            count = record.get_value()
            status_counts[status] = status_counts.get(status, 0) + count
    
    total = sum(status_counts.values())
    print("\nResults:")
    for status in sorted(status_counts.keys()):
        count = status_counts[status]
        percentage = (count / total * 100) if total > 0 else 0
        print(f"  {status:10s} : {count:>10,} records ({percentage:5.2f}%)")
    print(f"  {'TOTAL':10s} : {total:>10,} records")
    
    # Query 2: Sample attack records
    print("\n\n📋 QUERY 2: Sample Attack Records")
    print("-" * 80)
    
    query = '''
    from(bucket: "swat_db")
      |> range(start: 2015-01-01T00:00:00Z)
      |> filter(fn: (r) => r.status == "Attack")
      |> limit(n: 10)
    '''
    
    tables = query_api.query(query, org=INFLUX_ORG)
    print("\nFirst 10 attack records:")
    print(f"{'Time':26s} | {'Sensor':10s} | {'Value':>10s} | {'Status':10s}")
    print("-" * 80)
    
    for table in tables:
        for record in table.records:
            time = record.get_time().strftime('%Y-%m-%d %H:%M:%S')
            field = record.get_field()
            value = record.get_value()
            status = record.values.get('status', 'N/A')
            print(f"{time:26s} | {field:10s} | {value:>10.4f} | {status:10s}")
    
    # Query 3: Attack time range
    print("\n\n⏰ QUERY 3: Attack Timeline")
    print("-" * 80)
    
    query = '''
    from(bucket: "swat_db")
      |> range(start: 2015-01-01T00:00:00Z)
      |> filter(fn: (r) => r.status == "Attack")
      |> keep(columns: ["_time"])
    '''
    
    tables = query_api.query(query, org=INFLUX_ORG)
    times = []
    for table in tables:
        for record in table.records:
            times.append(record.get_time())
    
    if times:
        first_attack = min(times)
        last_attack = max(times)
        duration = last_attack - first_attack
        
        print(f"\nAttack Timeline:")
        print(f"  First attack:     {first_attack}")
        print(f"  Last attack:      {last_attack}")
        print(f"  Duration:         {duration}")
        print(f"  Total records:    {len(times):,}")
    
    # Query 4: Top sensors
    print("\n\n🎯 QUERY 4: Top 10 Sensors with Attack Data")
    print("-" * 80)
    
    query = '''
    from(bucket: "swat_db")
      |> range(start: 2015-01-01T00:00:00Z)
      |> filter(fn: (r) => r.status == "Attack")
      |> group(columns: ["_field"])
      |> count()
    '''
    
    tables = query_api.query(query, org=INFLUX_ORG)
    sensor_counts = {}
    for table in tables:
        for record in table.records:
            field = record.get_field()
            count = record.get_value()
            sensor_counts[field] = sensor_counts.get(field, 0) + count
    
    print("\nTop sensors by attack record count:")
    sorted_sensors = sorted(sensor_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    for i, (sensor, count) in enumerate(sorted_sensors, 1):
        print(f"  {i:2d}. {sensor:15s} : {count:>6,} records")
    
    print("\n" + "=" * 80)
    print("✅ QUERY COMPLETE - Attack data confirmed in InfluxDB!")
    print("=" * 80)
    
    client.close()

if __name__ == "__main__":
    main()
