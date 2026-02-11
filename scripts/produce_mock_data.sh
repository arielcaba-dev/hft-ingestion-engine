#!/bin/bash
# Create topic if not exists
docker exec redpanda rpk topic create market_data_raw -r 1 -p 1 || true

# Produce mock data
echo 'Producing mock data...'
echo '{"symbol_id": "BTC-USD", "exchange": "coinbase", "event_type": "Trade", "price": 50000.0, "quantity": 0.1, "time_exchange": "2023-10-27T10:00:00Z", "time_ingest": "2023-10-27T10:00:01Z", "is_snapshot": false, "sequence": 1}' | docker exec -i redpanda rpk topic produce market_data_raw
echo '{"symbol_id": "BTC-USD", "exchange": "coinbase", "event_type": "Trade", "price": 50100.0, "quantity": 0.2, "time_exchange": "2023-10-27T10:00:30Z", "time_ingest": "2023-10-27T10:00:31Z", "is_snapshot": false, "sequence": 2}' | docker exec -i redpanda rpk topic produce market_data_raw
echo '{"symbol_id": "BTC-USD", "exchange": "coinbase", "event_type": "Trade", "price": 49900.0, "quantity": 0.05, "time_exchange": "2023-10-27T10:01:00Z", "time_ingest": "2023-10-27T10:01:01Z", "is_snapshot": false, "sequence": 3}' | docker exec -i redpanda rpk topic produce market_data_raw
echo 'Data produced.'
