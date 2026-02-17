import boto3
import os

s3 = boto3.client('s3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    region_name='us-east-1'
)

bucket_name = 'market-data'

try:
    s3.create_bucket(Bucket=bucket_name)
    print(f"Bucket '{bucket_name}' created successfully.")
except Exception as e:
    # Check if bucket already exists (boto3 behavior varies by version)
    if 'BucketAlreadyOwnedByYou' in str(e):
        print(f"Bucket '{bucket_name}' already exists.")
    else:
        print(f"Error creating bucket: {e}")
