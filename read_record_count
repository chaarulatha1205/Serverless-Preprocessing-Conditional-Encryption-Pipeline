import boto3

def lambda_handler(event, context):
    s3 = boto3.client("s3")

    bucket = "production-output-curated-data"
    batch_id = event["batch_id"]

    prefix = f"output/batch_id={batch_id}/"

    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix
    )

    record_count = response.get("KeyCount", 0)

    return {
        "record_count": record_count
    }
