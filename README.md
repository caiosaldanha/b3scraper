# B3 Scraper

A project on aws that executes an scraping on B3 api and saves a parquet file with collected data on a S3 bucket. 

When the file arives at the s3 bucket it triggers a lambda function that also triggers a glue ETL job.

Scraper -> b3scrape.py

Lambda -> lambda_function.py

### Instructions

1. Create a .env file
2. Insert keys to use s3 resources
    - AWS_ACCESS_KEY_ID
    - AWS_SECRET_ACCESS_KEY
    - AWS_SESSION_TOKEN
    - S3_BUCKET
    - S3_BUCKET_PATH
3. Run the b3scrape.py file