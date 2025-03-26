import pandas as pd
import matplotlib.pyplot as plt
from influxdb_client import InfluxDBClient
from datetime import datetime, timedelta

# InfluxDB connection parameters
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "my-super-secret-token"
INFLUXDB_ORG = "myorg"

def query_metrics(metric_type):
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    query_api = client.query_api()
    
    # Query last 1 hour of data
    query = f'''
    from(bucket: "metrics")
        |> range(start: -1h)
        |> filter(fn: (r) => r["metric_type"] == "{metric_type}")
        |> filter(fn: (r) => r["_field"] == "usage_percent")
    '''
    
    result = query_api.query_data_frame(query)
    client.close()
    
    if result.empty:
        return pd.DataFrame()
    
    return result

def plot_metrics():
    # Get CPU and memory metrics
    cpu_data = query_metrics("cpu")
    memory_data = query_metrics("memory")
    
    if cpu_data.empty or memory_data.empty:
        print("No data available to plot")
        return
    
    # Create the plot
    plt.figure(figsize=(12, 6))
    
    plt.plot(cpu_data['_time'], cpu_data['_value'], label='CPU Usage')
    plt.plot(memory_data['_time'], memory_data['_value'], label='Memory Usage')
    
    plt.title('System Metrics Over Time')
    plt.xlabel('Time')
    plt.ylabel('Usage (%)')
    plt.legend()
    plt.grid(True)
    
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    plot_metrics() 