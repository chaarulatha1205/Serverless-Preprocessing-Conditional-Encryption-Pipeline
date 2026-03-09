# Serverless-Preprocessing-Conditional-Encryption-Pipeline

<img width="1218" height="1291" alt="Screenshot 2026-03-02 012247" src="https://github.com/user-attachments/assets/6ea5762f-cf4f-42e4-b3a9-cd37db1bc5b2" />

This project started with designing and implementing a serverless batch data pipeline using AWS Step Functions, AWS Glue, and AWS Lambda to efficiently process and manage data workflows.

🔹 Project Overview

This pipeline preprocesses raw data, evaluates record counts, conditionally performs encryption, and updates the AWS Glue Data Catalog. The orchestration ensures efficient processing while avoiding unnecessary compute costs.

⚙️ Workflow

1️⃣ Run preprocessing AWS Glue (PySpark) job

2️⃣ Use AWS Lambda to read the processed record count

3️⃣ Conditionally trigger an encryption Glue job using Step Functions Choice states

4️⃣ Update the AWS Glue Data Catalog

5️⃣ Gracefully exit if no data is present


🛠 Tech Stack

• AWS Step Functions

• AWS Glue (PySpark)

• AWS Lambda

• Amazon S3

• AWS Glue Data Catalog

✨ Key Features

✔ Conditional execution using Choice states

✔ Fully serverless architecture (no infrastructure management)

✔ Cost-efficient – avoids unnecessary encryption runs

✔ Production-safe orchestration with fault-tolerant workflow design


This architecture demonstrates how event-driven orchestration and serverless services can be combined to build scalable, efficient, and production-ready data pipelines on AWS.

