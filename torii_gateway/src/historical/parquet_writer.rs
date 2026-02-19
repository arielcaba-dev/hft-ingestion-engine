use crate::error::AppError;
use arrow::array::{Float64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use sqlx::Row;
use std::fs::File;
use std::sync::Arc;

pub fn generate_parquet_file(
    path: &str,
    rows: &[sqlx::postgres::PgRow],
    price_precision: f64,
    size_precision: f64,
) -> Result<(), AppError> {
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
        let symbol: String = row.get("symbol");
        let price: f64 = row.get("price");
        let quantity: f64 = row.get("quantity");
        let ts_micros: i64 = row.get("timestamp");

        // --- Precision Guard ---
        // Check if price aligns with tick size
        // e.g. price 100.01, tick 0.01 -> 10001 / 1 -> integer within float epsilon
        let price_rem = (price / price_precision).fract();
        if price_rem > 1e-9 && price_rem < (1.0 - 1e-9) {
             return Err(AppError::Internal(format!(
                 "Precision Invariant Failed: Price {} does not match tick size {}",
                 price, price_precision
             )));
        }

        // Check quantity aligns with lot size
        let qty_rem = (quantity / size_precision).fract();
        if qty_rem > 1e-9 && qty_rem < (1.0 - 1e-9) {
             return Err(AppError::Internal(format!(
                 "Precision Invariant Failed: Quantity {} does not match lot size {}",
                 quantity, size_precision
             )));
        }
        
        symbols.push(symbol);
        prices.push(price);
        quantities.push(quantity);
        timestamps.push(ts_micros);
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(symbols)),
            Arc::new(Float64Array::from(prices)),
            Arc::new(Float64Array::from(quantities)),
            Arc::new(TimestampMicrosecondArray::from(timestamps).with_timezone("UTC")),
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
