from influxdb_client import InfluxDBClient
from influxdb_client.client.tasks_api import TasksApi
import json

INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "my-super-secret-token"
INFLUXDB_ORG = "myorg"

def create_downsampling_task():
    """
    Creates a task to downsample data for long-term storage
    """
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    tasks_api = TasksApi(client)

    flux_script = '''
    option task = {
        name: "downsample_weather_data",
        every: 1m
    }

    data = from(bucket: "weather_metrics")
        |> range(start: -1m)
        |> filter(fn: (r) => r["_measurement"] == "weather_metrics")
        |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
        |> set(key: "downsampled", value: "1m")

    data
        |> to(bucket: "weather_metrics_downsampled")
    '''

    try:
        created_task = tasks_api.create_task(task_create_request={
            "org": INFLUXDB_ORG,
            "flux": flux_script,
            "name": "downsample_weather_data",
            "status": "active",
            "every": "1m"
        })
        print(f"Created task: {created_task.name}")
    except Exception as e:
        print(f"Error creating task: {e}")

def create_alert_task():
    """
    Creates a task to monitor for extreme weather conditions
    """
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    tasks_api = TasksApi(client)

    flux_script = '''
    option task = {
        name: "weather_alerts",
        every: 5m
    }

    threshold = from(bucket: "weather_metrics")
        |> range(start: -5m)
        |> filter(fn: (r) => r["_measurement"] == "weather_metrics")
        |> filter(fn: (r) => r["_field"] == "temperature")
        |> filter(fn: (r) => r["_value"] > 30.0 or r["_value"] < 0.0)

    threshold
        |> to(bucket: "weather_alerts")
    '''

    try:
        created_task = tasks_api.create_task(task_create_request={
            "org": INFLUXDB_ORG,
            "flux": flux_script,
            "name": "weather_alerts",
            "status": "active",
            "every": "5m"
        })
        print(f"Created task: {created_task.name}")
    except Exception as e:
        print(f"Error creating task: {e}")

def main():
    create_downsampling_task()
    create_alert_task()

if __name__ == "__main__":
    main() 