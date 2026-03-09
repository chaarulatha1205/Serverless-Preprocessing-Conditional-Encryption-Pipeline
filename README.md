# Serverless-Preprocessing-Conditional-Encryption-Pipeline
Overview
This project implements a serverless batch data pipeline using AWS Step Functions, AWS Glue, and AWS Lambda.

The pipeline preprocesses raw data, evaluates record counts, conditionally performs encryption, and updates the Glue Data Catalog.

Workflow
Run preprocessing Glue job
Read processed record count using Lambda
Conditionally execute encryption job
Update Glue Catalog
Gracefully exit if no data exists
Tech Stack
AWS Step Functions
AWS Glue (PySpark)
AWS Lambda
Amazon S3
AWS Glue Data Catalog
Key Features
Conditional execution using Choice states
Fully serverless architecture
Cost-efficient (no unnecessary encryption runs)
Production-safe orchestration
