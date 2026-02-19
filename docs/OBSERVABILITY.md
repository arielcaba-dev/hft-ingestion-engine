# HFT Ingestion Engine - Observability Guide

Learn how to monitor, debug, and inspect the data pipeline.

## 1. Grafana Dashboards
**URL**: `http://localhost:3000`
**Credentials**: `admin` / `admin`

- **Market Data & Risk Dashboard**: Visualizes real-time trade throughput, OHLCV candles, and CVaR 95% metrics.
- **Infrastructure Health**: Monitors Docker container resource usage (CPU/Memory) via Prometheus/cAdvisor integrations.

## 2. QuestDB (Data Persistence)
**Console UI**: `http://localhost:9000`
**Postgres Endpoint**: `localhost:8812` (User: `admin`, Pass: `quest`)

### Useful Queries
- **Row Count Verification**:
  ```sql
  SELECT count() FROM trades;
  SELECT count() FROM ohlcv_1m;
  ```
- **Latest Risk Metrics**:
  ```sql
  SELECT * FROM market_risk ORDER BY timestamp DESC LIMIT 10;
  ```
- **Check Ingestion Lag**:
  ```sql
  SELECT max(timestamp) - last_committed() FROM trades;
  ```

## 3. Arroyo (Stream Processing)
**Controller UI**: `http://localhost:5115`

- **Pipeline View**: Check the status of the `risk_engine_json_v1` pipeline.
- **Operator Metrics**: Monitor backpressure and event throughput at each join/aggregate step.
- **Log Inspection**: Use the UI or `docker logs arroyo` to see UDF compilation errors.

## 4. Redpanda (Kafka)
**RPK CLI**:
- **Check Topics**: `docker exec redpanda rpk topic list`
- **Consume Raw Stream**: `docker exec redpanda rpk topic consume market_data_raw -n 5`

## 5. Troubleshooting
- **Service Logs**: `docker compose logs -f <service_name>`
- **MinIO Cold Storage**: Access `http://localhost:9001` (admin/secret) to verify Parquet archives in `s3://archive/`.
