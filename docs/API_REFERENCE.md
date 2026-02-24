# HFT Ingestion Engine - API Reference

The Torii Gateway provides a unified REST and WebSocket API for real-time and historical market data access.

## Base URL
`http://www.toriidata.tech:8080` (External)
`http://gateway:8080` (Internal Docker)

## Authentication
All requests require an API key passed via the `X-API-KEY` header.
- **Header**: `X-API-KEY: <your_api_key>`
- **Query Param (WebSocket)**: `token=<your_api_key>`

---

## REST API Endpoints

### 1. Market Health
`GET /v1/market/health`
Check connectivity to QuestDB and underlying data streams.

**Response (200 OK)**:
```json
{
  "status": "healthy",
  "questdb": "connected",
  "last_update": "2026-02-19T05:20:00Z"
}
```

### 2. Historical Trades
`GET /v1/trades/historical?symbol={symbol}&limit={limit}`

Returns historical trade data.
- **symbol**: e.g., `BTC-USD`
- **limit**: (Optional) Max rows to return. Default is `total_count`.

**Behavior**:
- **Small Requests (< 10,000 rows)**: Returns JSON array directly.
- **Large Requests (> 10,000 rows)**: Returns `303 See Other` with a Redirect to a presigned Parquet download link on MinIO.

**JSON Response Example**:
```json
[
  {
    "symbol": "BTC-USD",
    "price": 67467.68,
    "quantity": 0.002,
    "timestamp": "2026-02-18T00:28:38.444Z"
  }
]
```

### 3. Risk Metrics
`GET /v1/market/risk?symbol={symbol}`

Fetch unified real-time risk calculations combining Arroyo metrics and DeFi indicators.

**Response (200 OK)**:
```json
{
  "symbol": "BTC-USD",
  "volatility": 0.0012,
  "liquidity": 450000.0,
  "rsi": 55.4,
  "cvar_95": -0.024,
  "il_score": -0.000027,
  "entry_price": 67288.39,
  "current_price": 67320.0
}
```

---

### 4. Open Interest
`GET /v1/derivatives/oi/{symbol}`

Fetch the latest Open Interest snapshots from Binance Futures (polled every 60s).

**Response (200 OK)**:
```json
{
  "symbol": "BTC-USD",
  "count": 1,
  "open_interest": [
    {
      "timestamp": "2026-02-24T09:09:04Z",
      "exchange": "binance_futures",
      "oi_value": 81671.819,
      "notional_value": 5163872267.09
    }
  ]
}
```

### 5. Liquidations
`GET /v1/derivatives/liquidations/{symbol}`

Fetch recent forced-closure events from Binance Futures perpetual markets.

**Response (200 OK)**:
```json
{
  "symbol": "ETH-USD",
  "count": 2,
  "liquidations": [
    {
      "timestamp": "2026-02-24T09:09:52Z",
      "exchange": "binance_futures",
      "side": "Long",
      "price": 1819.00,
      "quantity": 3.443,
      "notional": 6262.82
    }
  ]
}
```

---

## WebSocket API

`WS /v1/ws?token=<key>`

### Subscribe
```json
{
  "action": "subscribe",
  "symbols": ["BTC-USD"]
}
```

---

## OpenAPI 3.0 Specification (Snippet)

```yaml
openapi: 3.0.0
info:
  title: Torii HFT Gateway
  version: 1.0.0
paths:
  /v1/trades/historical:
    get:
      summary: Get historical trades
      parameters:
        - name: symbol
          in: query
          required: true
          schema:
            type: string
        - name: limit
          in: query
          schema:
            type: integer
      responses:
        '200':
          description: JSON result for small requests
        '303':
          description: Redirect to S3/MinIO for large requests
```
