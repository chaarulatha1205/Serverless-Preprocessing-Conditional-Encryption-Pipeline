import boto3

glue = boto3.client("glue")

CRAWLER_NAME = "encrypted_data_crawler"   # Must match exactly

def lambda_handler(event, context):
    try:
        print("Starting crawler:", CRAWLER_NAME)

        glue.start_crawler(Name=CRAWLER_NAME)

        return {
            "status": "CRAWLER_STARTED"
        }

    except glue.exceptions.CrawlerRunningException:
        print("Crawler already running")

        return {
            "status": "CRAWLER_ALREADY_RUNNING"
        }

    except glue.exceptions.EntityNotFoundException:
        print("Crawler not found")

        raise Exception(f"Crawler {CRAWLER_NAME} does not exist")

    except Exception as e:
        print("Unexpected error:", str(e))
        raise e
