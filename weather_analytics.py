from influxdb_client import InfluxDBClient
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta

INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "my-super-secret-token"
INFLUXDB_ORG = "myorg"

class WeatherAnalytics:
    def __init__(self):
        self.client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
        self.query_api = self.client.query_api()

    def get_downsampled_data(self, window='1h'):
        """
        Demonstrates downsampling data using Flux window functions
        """
        query = f'''
        from(bucket: "weather_metrics")
            |> range(start: -24h)
            |> filter(fn: (r) => r["_measurement"] == "weather_metrics")
            |> filter(fn: (r) => r["_field"] == "temperature" or r["_field"] == "humidity")
            |> aggregateWindow(every: {window}, fn: mean, createEmpty: false)
            |> pivot(rowKey:["_time", "station_id"], columnKey: ["_field"], valueColumn: "_value")
        '''
        return self.query_api.query_data_frame(query)

    def calculate_heat_index(self):
        """
        Demonstrates complex calculations using Flux
        """
        query = '''
        temperature = from(bucket: "weather_metrics")
            |> range(start: -1h)
            |> filter(fn: (r) => r["_field"] == "temperature")
            |> keep(columns: ["_time", "_value", "station_id"])
            |> rename(columns: {_value: "temp_value"})

        humidity = from(bucket: "weather_metrics")
            |> range(start: -1h)
            |> filter(fn: (r) => r["_field"] == "humidity")
            |> keep(columns: ["_time", "_value", "station_id"])
            |> rename(columns: {_value: "hum_value"})

        join(
            tables: {temp: temperature, hum: humidity},
            on: ["_time", "station_id"]
        )
            |> map(fn: (r) => ({
                _time: r._time,
                station_id: r.station_id,
                _value:
                    -42.379 +
                    2.04901523 * r.temp_value +
                    10.14333127 * r.hum_value +
                    -0.22475541 * r.temp_value * r.hum_value +
                    -0.00683783 * r.temp_value * r.temp_value +
                    -0.05481717 * r.hum_value * r.hum_value +
                    0.00122874 * r.temp_value * r.temp_value * r.hum_value +
                    0.00085282 * r.temp_value * r.hum_value * r.hum_value +
                    -0.00000199 * r.temp_value * r.temp_value * r.hum_value * r.hum_value,
                _field: "heat_index",
                _measurement: "weather_metrics"
            }))
        '''
        return self.query_api.query_data_frame(query)

    def analyze_weather_patterns(self):
        """
        Demonstrates advanced aggregations and pattern detection
        """
        query = '''
        from(bucket: "weather_metrics")
            |> range(start: -24h)
            |> filter(fn: (r) => r["_field"] == "temperature")
            |> aggregateWindow(every: 1h, fn: mean)
            |> derivative(unit: 1h, nonNegative: false)
            |> map(fn: (r) => ({
                r with 
                trend: if r._value > 0.0 then "rising"
                    else if r._value < 0.0 then "falling"
                    else "stable"
            }))
            |> pivot(rowKey:["_time", "station_id"], columnKey: ["_field"], valueColumn: "_value")
        '''
        return self.query_api.query_data_frame(query)

    def visualize_correlations(self):
        """
        Create correlation analysis between different weather metrics
        """
        query = '''
        from(bucket: "weather_metrics")
            |> range(start: -24h)
            |> filter(fn: (r) => r["_field"] == "temperature" or r["_field"] == "humidity" or r["_field"] == "pressure" or r["_field"] == "wind_speed")
            |> pivot(rowKey:["_time", "station_id"], columnKey: ["_field"], valueColumn: "_value")
            |> keep(columns: ["_time", "station_id", "temperature", "humidity", "pressure", "wind_speed"])
        '''
        df = self.query_api.query_data_frame(query)
        
        # Drop non-numeric columns and any rows with missing values
        numeric_columns = ["temperature", "humidity", "pressure", "wind_speed"]
        df_clean = df[numeric_columns].dropna()
        
        # Create correlation matrix
        corr_matrix = df_clean.corr()
        
        # Plot correlation heatmap
        plt.figure(figsize=(10, 8))
        sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', vmin=-1, vmax=1)
        plt.title('Weather Metrics Correlation Matrix')
        plt.show()

def main():
    analytics = WeatherAnalytics()
    
    # Demonstrate different analyses
    print("Analyzing downsampled data...")
    downsampled_data = analytics.get_downsampled_data()
    print(downsampled_data.head())
    
    print("\nCalculating heat index...")
    heat_index = analytics.calculate_heat_index()
    print(heat_index.head())
    
    print("\nAnalyzing weather patterns...")
    patterns = analytics.analyze_weather_patterns()
    print(patterns.head())
    
    print("\nGenerating correlation visualization...")
    analytics.visualize_correlations()

if __name__ == "__main__":
    main() 