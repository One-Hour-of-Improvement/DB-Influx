import psutil
import time
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# InfluxDB connection parameters
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "my-super-secret-token"
INFLUXDB_ORG = "myorg"
INFLUXDB_BUCKET = "metrics"

def collect_metrics():
    # Get CPU usage
    cpu_percent = psutil.cpu_percent(interval=1)
    
    # Get memory usage
    memory = psutil.virtual_memory()
    memory_percent = memory.percent
    
    return cpu_percent, memory_percent

def write_to_influxdb(cpu_percent, memory_percent):
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    # Create points for CPU metrics
    cpu_point = Point("system_metrics")\
        .tag("host", "localhost")\
        .tag("metric_type", "cpu")\
        .field("usage_percent", cpu_percent)
    
    # Create points for memory metrics
    memory_point = Point("system_metrics")\
        .tag("host", "localhost")\
        .tag("metric_type", "memory")\
        .field("usage_percent", memory_percent)
    
    # Write points to InfluxDB
    write_api.write(bucket=INFLUXDB_BUCKET, record=[cpu_point, memory_point])
    client.close()

def main():
    print("Starting metrics collection...")
    while True:
        try:
            cpu_percent, memory_percent = collect_metrics()
            write_to_influxdb(cpu_percent, memory_percent)
            print(f"CPU Usage: {cpu_percent}% | Memory Usage: {memory_percent}%")
            time.sleep(1)  # Collect metrics every 1 seconds
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(1)

if __name__ == "__main__":
    main() 