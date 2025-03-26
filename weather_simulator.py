import random
import time
from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# InfluxDB connection parameters
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "my-super-secret-token"
INFLUXDB_ORG = "myorg"
INFLUXDB_BUCKET = "weather_metrics"

# Simulate multiple weather stations
STATIONS = {
    'station_1': {'lat': 40.7128, 'lon': -74.0060, 'elevation': 10, 'location': 'New York'},
    'station_2': {'lat': 34.0522, 'lon': -118.2437, 'elevation': 71, 'location': 'Los Angeles'},
    'station_3': {'lat': 41.8781, 'lon': -87.6298, 'elevation': 182, 'location': 'Chicago'}
}

class WeatherSimulator:
    def __init__(self):
        self.client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        
        # Base values for each station
        self.base_temps = {station: random.uniform(15, 25) for station in STATIONS.keys()}
        self.base_humidity = {station: random.uniform(40, 60) for station in STATIONS.keys()}
        self.base_pressure = {station: random.uniform(1010, 1020) for station in STATIONS.keys()}

    def generate_weather_data(self):
        timestamp = datetime.utcnow()
        points = []

        for station_id, station_info in STATIONS.items():
            # Add some random variations to base values
            temperature = self.base_temps[station_id] + random.uniform(-2, 2)
            humidity = min(100, max(0, self.base_humidity[station_id] + random.uniform(-5, 5)))
            pressure = self.base_pressure[station_id] + random.uniform(-1, 1)
            wind_speed = random.uniform(0, 20)
            wind_direction = random.uniform(0, 360)
            # Convert precipitation to integer to maintain consistent type
            precipitation = int(random.uniform(0, 5)) if random.random() < 0.3 else 0
            
            # Create rich metadata point
            point = Point("weather_metrics")\
                .tag("station_id", station_id)\
                .tag("location", station_info['location'])\
                .tag("sensor_type", "weather_station")\
                .field("temperature", temperature)\
                .field("humidity", humidity)\
                .field("pressure", pressure)\
                .field("wind_speed", wind_speed)\
                .field("wind_direction", wind_direction)\
                .field("precipitation", precipitation)\
                .field("latitude", station_info['lat'])\
                .field("longitude", station_info['lon'])\
                .field("elevation", station_info['elevation'])\
                .time(timestamp)
            
            points.append(point)
            
            # Update base values with some drift
            self.base_temps[station_id] += random.uniform(-0.1, 0.1)
            self.base_humidity[station_id] += random.uniform(-0.2, 0.2)
            self.base_pressure[station_id] += random.uniform(-0.1, 0.1)

        return points

    def write_data(self, points):
        try:
            self.write_api.write(bucket=INFLUXDB_BUCKET, record=points)
            print(f"Written {len(points)} points at {datetime.now()}")
        except Exception as e:
            print(f"Error writing to InfluxDB: {e}")

def main():
    simulator = WeatherSimulator()
    
    print("Starting weather simulation...")
    while True:
        points = simulator.generate_weather_data()
        simulator.write_data(points)
        time.sleep(1)  # Generate data every 1 seconds

if __name__ == "__main__":
    main() 