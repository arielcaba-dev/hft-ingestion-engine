CREATE SINK iceberg_sink
FROM market_data_raw
WITH (
    connector = 'iceberg',
    type = 'append-only',
    force_append_only = 'true',
    database.name = 'demo_db',
    table.name = 'market_data_raw',
    catalog.type = 'storage', -- Using storage/filesystem catalog for simple S3 without Rest/Hive
    warehouse.path = 's3a://hft-datalake/iceberg',
    s3.endpoint = 'http://minio:9000',
    s3.access.key = 'minioadmin',
    s3.secret.key = 'minioadmin',
    s3.region = 'us-east-1'
);
