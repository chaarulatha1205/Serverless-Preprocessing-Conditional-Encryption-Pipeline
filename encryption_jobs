import sys
import pandas as pd
import hashlib
import s3fs

# ---------------------------------
# Read Glue Arguments
# ---------------------------------
args = sys.argv

def get_arg(name):
    if name in args:
        return args[args.index(name) + 1]
    return None

batch_id = get_arg("--batch_id")

if not batch_id:
    raise Exception("Missing required parameter: --batch_id")

# ---------------------------------
# Read Processed Parquet Output
# ---------------------------------
bucket = "production-output-curated-data"
prefix = f"output/batch_id={batch_id}/"

fs = s3fs.S3FileSystem()
files = fs.glob(f"{bucket}/{prefix}*.parquet")

if not files:
    raise Exception("No processed parquet files found")

df_list = []

for file in files:
    print("Reading:", file)
    df_list.append(pd.read_parquet(f"s3://{file}"))

df = pd.concat(df_list, ignore_index=True)

if df.empty:
    raise Exception("Processed dataset is empty")

print("Rows loaded:", len(df))

# ---------------------------------
# SHA256 Hash Function
# ---------------------------------
def sha256_hash(value):
    if pd.isna(value):
        return None
    return hashlib.sha256(str(value).encode()).hexdigest()

# ---------------------------------
# Sensitive Columns
# ---------------------------------
sensitive_columns = [
    "customer_email",
    "customer_phone",
    "credit_card_number"
]

missing_sensitive = [col for col in sensitive_columns if col not in df.columns]

if missing_sensitive:
    raise Exception(f"Missing sensitive columns: {missing_sensitive}")

for col in sensitive_columns:
    print(f"Encrypting column: {col}")
    df[col] = df[col].apply(sha256_hash)

print("Encryption applied successfully")

# ---------------------------------
# Write Encrypted Output (Parquet)
# ---------------------------------
output_path = f"s3://production-encrypted-data/encrypted/batch_id={batch_id}/encrypted_data.parquet"

df.to_parquet(output_path, index=False)

print("Encrypted file written to:", output_path)
