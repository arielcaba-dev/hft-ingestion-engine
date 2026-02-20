import aiohttp
import pandas as pd
import asyncio
from typing import Optional, Dict, Any, List

class ToriiClient:
    """
    Async Python SDK for Torii Data HFT Platform.
    """
    def __init__(self, api_key: str, base_url: str = "http://localhost:8080"):
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {
            "X-API-KEY": self.api_key,
            "Content-Type": "application/json"
        }

    async def _get(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.base_url}{endpoint}", headers=self.headers, params=params) as response:
                if response.status != 200:
                    text = await response.text()
                    raise Exception(f"API Error {response.status}: {text}")
                return await response.json()

    async def _post(self, endpoint: str, json: Dict[str, Any]) -> Dict[str, Any]:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self.base_url}{endpoint}", headers=self.headers, json=json) as response:
                if response.status != 200:
                    text = await response.text()
                    raise Exception(f"API Error {response.status}: {text}")
                return await response.json()

    async def get_ohlcv(self, symbol: str, timeframe: str = "1m", limit: int = 100) -> pd.DataFrame:
        """
        Fetch OHLCV data and restore as a Pandas DataFrame.
        """
        data = await self._get("/v1/market/ohlcv", params={"symbol": symbol, "timeframe": timeframe, "limit": limit})
        df = pd.DataFrame(data)
        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
        return df

    async def get_risk_metrics(self, symbol: str) -> pd.DataFrame:
        """
        Fetch real-time risk metrics (VaR, Volatility).
        """
        # Note: Depending on API, this might return a single object or list.
        # Assuming list for DataFrame compatibility.
        data = await self._post("/v1/mcp", json={"query": f"Analyze risk for {symbol}", "context": {"symbol": symbol}})
        
        # Extract data from MCP response wrapper
        if "data" in data:
            records = data["data"]
            df = pd.DataFrame(records)
            if not df.empty and 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df.set_index('timestamp', inplace=True)
            return df
        return pd.DataFrame()

    async def query_mcp(self, query: str, symbol: str = None) -> Dict[str, Any]:
        """
        Send a natural language query to the Model Context Protocol (MCP).
        """
        context = {}
        if symbol:
            context["symbol"] = symbol
            
        return await self._post("/v1/mcp", json={"query": query, "context": context})

    async def get_sentiment(self, symbol: str) -> pd.DataFrame:
        """
        Fetch sentiment data.
        """
        data = await self._post("/v1/mcp", json={"query": f"Check sentiment for {symbol}", "context": {"symbol": symbol}})
        if "data" in data:
            df = pd.DataFrame(data["data"])
            if not df.empty and 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df.set_index('timestamp', inplace=True)
            return df
        return pd.DataFrame()
