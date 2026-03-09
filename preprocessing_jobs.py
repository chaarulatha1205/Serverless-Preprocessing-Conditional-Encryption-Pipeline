import sys
import pandas as pd
from datetime import datetime

# -------------------------------
# Read Glue Job Arguments
# -------------------------------
args = sys.argv

def get_arg(name):
    if name in args:
        return args[args.index(name) + 1]
    return None

bucket_name = get_arg("--bucket_name")
file_key = get_arg("--file_key")
batch_id = get_arg("--batch_id")
timestamp = get_arg("--timestamp")

if not bucket_name or not file_key:
    raise Exception("Missing required job parameters")

input_path = f"s3://{bucket_name}/{file_key}"

print("Reading file:", input_path)
print("Batch ID:", batch_id)

# -------------------------------
# Read File (Auto detect format)
# -------------------------------
if file_key.endswith(".csv"):
    df = pd.read_csv(input_path)
elif file_key.endswith(".parquet"):
    df = pd.read_parquet(input_path)
else:
    raise Exception("Unsupported file format")

if df.empty:
    raise Exception("Input file is empty")

print("Initial row count:", len(df))

# -------------------------------
# Validate Required Columns
# -------------------------------
required_columns = ["order_id", "customer_id", "product", "amount", "order_date"]

missing_cols = set(required_columns) - set(df.columns)
if missing_cols:
    raise Exception(f"Missing required columns: {missing_cols}")

# -------------------------------
# Remove Nulls
# -------------------------------
df = df.dropna(subset=required_columns)

# -------------------------------
# Standardization
# -------------------------------
df["product"] = df["product"].astype(str).str.strip().str.lower()
df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
df = df[df["order_date"].notna()]
df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
df = df[df["amount"] >= 0]

# -------------------------------
# Window-style De-duplication
# Keep latest order_date per order_id
# -------------------------------
df = df.sort_values(by="order_date", ascending=False)
df = df.drop_duplicates(subset=["order_id"], keep="first")

# -------------------------------
# Add processed timestamp
# -------------------------------
df["processed_ts"] = datetime.utcnow()

print("Final row count:", len(df))

# -------------------------------
# Write Output (Batch partitioned)
# -------------------------------
output_path = f"s3://production-output-curated-data/output/batch_id={batch_id}/cleaned_data.parquet"

df.to_parquet(output_path, index=False)

print("File written to:", output_path)
