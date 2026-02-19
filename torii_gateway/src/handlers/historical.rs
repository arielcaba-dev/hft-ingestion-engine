use crate::billing::deduct_credits;
use crate::error::AppError;
use crate::model::AuthContext;
use crate::state::AppState;
use arrow::array::{Float64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use axum::{
    extract::{Query, State},
    response::{IntoResponse, Redirect, Response},
    Json,
};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde::Deserialize;
use sqlx::Row;
use std::fs::File;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Deserialize)]
pub struct HistoricalParams {
    pub symbol: String,
    pub start: Option<String>,
    pub end: Option<String>,
    pub limit: Option<i64>,
}

pub async fn historical_handler(
    State(state): State<Arc<AppState>>,
    auth: AuthContext,
    Query(params): Query<HistoricalParams>,
) -> Result<Response, AppError> {
    // 1. Calculate Cost base fee
    let base_cost = 10;
    deduct_credits(&state, auth.user_id, base_cost)
        .await
        .map_err(|e| match e {
            axum::http::StatusCode::PAYMENT_REQUIRED => {
                AppError::PaymentRequired("Insufficient credits".into())
            }
            _ => AppError::Internal("Billing error".into()),
        })?;

    // 2. Query QuestDB for row count
    // We use the PG pool for QuestDB
    let count_query = "SELECT count() FROM trades WHERE symbol = $1";
    let total_count: i64 = sqlx::query_scalar(count_query)
        .bind(&params.symbol)
        .fetch_one(&state.questdb)
        .await
        .map_err(|e| AppError::Internal(format!("QuestDB count error: {}", e)))?;

    let effective_limit = params.limit.unwrap_or(total_count);
    
    if total_count > 10_000 && effective_limit > 10_000 {
        // --- Large Request Path (S3 Offload) ---
        
        // 1. Check Cache
        let cache_key = format!("hist_cache:{}:{}", params.symbol, effective_limit);
        let mut conn = state.redis.get_multiplexed_async_connection().await.map_err(|_| AppError::Internal("Redis error".into()))?;
        let cached_filename: Option<String> = redis::AsyncCommands::get(&mut conn, &cache_key).await.unwrap_or(None);

        if let Some(filename) = cached_filename {
            let presigned_req = state.s3_client
                .get_object()
                .bucket(&state.config.s3_bucket)
                .key(&filename)
                .presigned(aws_sdk_s3::presigning::PresigningConfig::expires_in(std::time::Duration::from_secs(3600)).unwrap())
                .await
                .map_err(|e| AppError::Internal(format!("Presigned URL error: {}", e)))?;

            return Ok(Redirect::to(&presigned_req.uri().to_string()).into_response());
        }

        let filename = format!(
            "{}_{}_{}.parquet",
            params.symbol,
            Uuid::new_v4(),
            chrono::Utc::now().timestamp()
        );
        let filepath = format!("/tmp/{}", filename);

        // A. Fetch data from QuestDB
        let data_query = "SELECT symbol, price, quantity, CAST(timestamp AS BIGINT) as timestamp FROM trades WHERE symbol = $1 ORDER BY timestamp DESC LIMIT $2";
        let rows = sqlx::query(data_query)
            .bind(&params.symbol)
            .bind(params.limit.unwrap_or(50000)) // Use limit or default for offload
            .fetch_all(&state.questdb)
            .await
            .map_err(|e| AppError::Internal(format!("QuestDB data error: {}", e)))?;

        // B. Generate Parquet File
        generate_parquet_file(&filepath, &rows)?;

        // C. Upload to S3/MinIO
        let body = aws_sdk_s3::primitives::ByteStream::from_path(&filepath)
            .await
            .map_err(|e| AppError::Internal(format!("Failed to read parquet file: {}", e)))?;

        state.s3_client
            .put_object()
            .bucket(&state.config.s3_bucket)
            .key(&filename)
            .body(body)
            .send()
            .await
            .map_err(|e| AppError::Internal(format!("S3 Upload error: {}", e)))?;

        // D. Generate Presigned URL
        let presigned_req = state.s3_client
            .get_object()
            .bucket(&state.config.s3_bucket)
            .key(&filename)
            .presigned(aws_sdk_s3::presigning::PresigningConfig::expires_in(std::time::Duration::from_secs(3600)).unwrap())
            .await
            .map_err(|e| AppError::Internal(format!("Presigned URL error: {}", e)))?;

        let presigned_url = presigned_req.uri().to_string();

        // E. Cache the result for 60 seconds
        let _: () = redis::AsyncCommands::set_ex(&mut conn, &cache_key, &filename, 60).await.unwrap_or(());

        // Optional: Cleanup local file
        let _ = std::fs::remove_file(&filepath);

        // Return 303 See Other
        Ok(Redirect::to(&presigned_url).into_response())
    } else {
        // --- Small Request Path (JSON Stream) ---
        let data_query = "SELECT symbol, price, quantity, CAST(timestamp AS BIGINT) as timestamp FROM trades WHERE symbol = $1 ORDER BY timestamp DESC LIMIT $2";
        let rows = sqlx::query(data_query)
            .bind(&params.symbol)
            .bind(params.limit.unwrap_or(10000))
            .fetch_all(&state.questdb)
            .await
            .map_err(|e| AppError::Internal(format!("QuestDB data error: {}", e)))?;

        let results: Vec<serde_json::Value> = rows.into_iter().map(|row| {
            let ts_micros = row.get::<i64, _>("timestamp");
            let dt = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
                chrono::NaiveDateTime::from_timestamp_micros(ts_micros).unwrap_or_default(),
                chrono::Utc
            );
            serde_json::json!({
                "symbol": row.get::<String, _>("symbol"),
                "price": row.get::<f64, _>("price"),
                "quantity": row.get::<f64, _>("quantity"),
                "timestamp": dt.to_rfc3339()
            })
        }).collect();

        Ok(Json(results).into_response())
    }
}

fn generate_parquet_file(path: &str, rows: &[sqlx::postgres::PgRow]) -> Result<(), AppError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("quantity", DataType::Float64, false),
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
    ]));

    let mut symbols = Vec::new();
    let mut prices = Vec::new();
    let mut quantities = Vec::new();
    let mut timestamps = Vec::new();

    for row in rows {
        symbols.push(row.get::<String, _>("symbol"));
        prices.push(row.get::<f64, _>("price"));
        quantities.push(row.get::<f64, _>("quantity"));
        
        let ts_micros = row.get::<i64, _>("timestamp");
        timestamps.push(ts_micros);
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(symbols)),
            Arc::new(Float64Array::from(prices)),
            Arc::new(Float64Array::from(quantities)),
            Arc::new(TimestampMicrosecondArray::from_vec(timestamps, Some("UTC".into()))),
        ],
    ).map_err(|e| AppError::Internal(format!("Arrow batch error: {}", e)))?;

    let file = File::create(path).map_err(|e| AppError::Internal(e.to_string()))?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))
        .map_err(|e| AppError::Internal(format!("Parquet writer error: {}", e)))?;

    writer.write(&batch).map_err(|e| AppError::Internal(format!("Parquet write error: {}", e)))?;
    writer.close().map_err(|e| AppError::Internal(format!("Parquet close error: {}", e)))?;

    Ok(())
}
