# Influx DB

InfluxDB is an open-source time series database that is used to store and query time series data. It is used in conjunction with other technologies like Grafana, Telegraf, and Kapacitor to store and process large datasets. It boast it's capability to handle high write and query loads (1m+ writes/sec).

## Installation

```bash
docker-compose up -d

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## What I learnt

- What is InfluxDB?
- How to publish data point to InfluxDB
- How to query data from InfluxDB using SQL and FluxQL
- How to create dashboard in Grafana
- How to create task in InfluxDB
- How to create alert in InfluxDB
- How to do advance aggregation in InfluxDB
---

## Metadata

| Key | Value |
|-----|--------|
| # of 1 Hour of Improvement | 42 |
| Learnt at | 2025-03-26 |