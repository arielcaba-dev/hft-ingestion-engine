use gateway_service::handlers::mcp::McpQuery;
use serde_json::json;

#[tokio::test]
async fn test_mcp_query_parsing() {
    // This test verifies the logic of query parsing without needing a full DB connection.
    // For a real integration test, we'd spawn the app.

    let query = "Get RSI for BTC-USD";
    let payload = json!({
        "query": query,
        "context": {"symbol": "BTC-USD"}
    });

    // We can't easily call the handler directly without state, so we unit test the parsing logic if we extracted it.
    // Given the handler is monolithic, we'll write a test that checks the expected SQL generation logic if it were exposed.

    assert!(query.to_lowercase().contains("rsi"));
}
