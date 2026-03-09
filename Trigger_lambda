import json
import boto3
import uuid
from datetime import datetime
import urllib.parse

# Initialize Step Functions client
stepfunctions = boto3.client("stepfunctions")

# Replace with your Step Function ARN
STATE_MACHINE_ARN = "arn:aws:states:ap-south-1:632674124162:stateMachine:orchestration"

def lambda_handler(event, context):

    # Extract S3 details
    record = event["Records"][0]
    bucket_name = record["s3"]["bucket"]["name"]
    file_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

    file_name = file_key.split("/")[-1]

    # Generate batch ID
    batch_id = str(uuid.uuid4())

    # Current UTC timestamp
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Prepare payload for Step Function
    input_payload = {
        "bucket_name": bucket_name,
        "file_key": file_key,
        "file_name": file_name,
        "batch_id": batch_id,
        "timestamp": timestamp
    }

    print("Starting Step Function with payload:")
    print(json.dumps(input_payload, indent=2))

    # Start execution
    response = stepfunctions.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        name=f"{batch_id}",
        input=json.dumps(input_payload)
    )

    print("Execution ARN:", response["executionArn"])

    return {
        "statusCode": 200,
        "body": json.dumps("Step Function triggered successfully")
    }
