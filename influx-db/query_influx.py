import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

from influxdb_client import InfluxDBClient
import pandas as pd
from typing import Optional

# --- Configuration ---
# NOTE: You have TWO InfluxDB instances running:
# 1. Port 8086 (InfluxDB 2.x) - contains only monitoring metrics
# 2. Port 8181 (InfluxDB 3.x) - should contain SWaT data from simple_swat_consumer.py

# Using InfluxDB 2.x configuration (from write_kafka_to_influx.py)
INFLUX_ORG = "ucs"
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "QxzNbxYC3mh15AOYK5e1rweiLzHsS9WqlCDvZ9VTRdTcvVO3gbp2FexttgNzwecf06BdA5On8SSPu6Ky1UBDXQ=="
INFLUX_BUCKET = "swat_db"


def query_swat_data(
    limit: int = 100,
    time_range: str = "2015-01-01T00:00:00Z",  # Start from 2015 to capture CSV data
    measurement: str = "swat_sensor_data",
    status_filter: Optional[str] = None
) -> pd.DataFrame:
    
    print(f"\nAttempting to connect to InfluxDB at {INFLUX_URL}")
    print(f"Organization: {INFLUX_ORG}")
    print(f"Bucket: {INFLUX_BUCKET}")
    
    # Create client
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    query_api = client.query_api()

    # Build Flux query
    status_filter_clause = ""
    if status_filter:
        status_filter_clause = f'|> filter(fn: (r) => r.status == "{status_filter}")'

    query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: {time_range})
      |> filter(fn: (r) => r._measurement == "{measurement}")
      {status_filter_clause}
      |> limit(n: {limit})
    '''

    print(f"Querying InfluxDB for {measurement}...")
    
    try:
        # Execute query
        tables = query_api.query(query, org=INFLUX_ORG)

        # Convert to pandas DataFrame
        results = []
        for table in tables:
            for record in table.records:
                results.append({
                    'time': record.get_time(),
                    'field': record.get_field(),
                    'value': record.get_value(),
                    'status': record.values.get('status', 'N/A')
                })

        df = pd.DataFrame(results)
        return df
    
    except Exception as e:
        print(f"\nError executing query: {str(e)}")
        print("\nDebug information:")
        print(f"Query being executed:\n{query}")
        return pd.DataFrame()
    
    finally:
        try:
            client.close()
        except Exception as e:
            print(f"Error closing client: {str(e)}")


def print_query_summary(df: pd.DataFrame) -> None:
    """Print a summary of the query results"""
    if df.empty:
        print("No data found in InfluxDB")
        return

    print(f"\nRetrieved {len(df)} records from InfluxDB")
    print("\nFirst 10 records:")
    print(df.head(10))
    
    print("\nData summary:")
    print(f"  Time range: {df['time'].min()} to {df['time'].max()}")
    print(f"  Unique fields: {df['field'].nunique()}")
    print(f"  Total records per field:")
    print(df['field'].value_counts().head(10))
    print(f"\n  Status distribution:")
    print(df['status'].value_counts())


def get_latest_records(limit: int = 10) -> pd.DataFrame:
    """Get the most recent records"""
    return query_swat_data(limit=limit, time_range="2015-01-01T00:00:00Z")


def get_sensor_data(sensor_name: str, limit: int = 100) -> pd.DataFrame:
    """Get data for a specific sensor field"""
    df = query_swat_data(limit=limit * 60)  # Get more records to filter
    if not df.empty:
        return df[df['field'] == sensor_name]
    return df


def get_attack_data(limit: int = 100) -> pd.DataFrame:
    """Get only attack/anomaly records"""
    return query_swat_data(limit=limit, status_filter="Attack")


def main():
    """Main entry point for querying InfluxDB"""
    print("=" * 60)
    print("SWaT Sensor Data Query Tool")
    print("=" * 60)
    
    # Query all recent data (from 2015 onwards to capture CSV data)
    df = query_swat_data(limit=100, time_range="2015-01-01T00:00:00Z")
    print_query_summary(df)


if __name__ == "__main__":
    main()