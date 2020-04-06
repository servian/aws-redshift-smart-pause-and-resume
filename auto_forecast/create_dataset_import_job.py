import json
import boto3
import logging 
import datetime as dt
import os

from utils.forecast_helpers import derive_dataset_arn

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    DATASET_NAME = os.environ["DATASET_NAME"]
    BUCKET_NAME = os.environ["BUCKET_NAME"]
    FORECAST_ROLE_ARN = os.environ["FORECAST_ROLE_ARN"] # to allow access to s3 bucket 
    DATASET_IMPORT_JOB_NAME = "{0}_import{1}".format(DATASET_NAME, dt.datetime.now().strftime("%Y%m%d"))
    DATASET_ARN = derive_dataset_arn(dataset_name = DATASET_NAME, lambda_function_arn = context.invoked_function_arn)
    s3_OBJECT_KEY = "s3://{0}/".format(BUCKET_NAME)
    TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss" # can add this as another variable in template
    
    try:
        forecast_client = boto3.client("forecast")
        
        forecast_client.create_dataset_import_job(
            DatasetImportJobName = DATASET_IMPORT_JOB_NAME,
            DatasetArn = DATASET_ARN,
            DataSource = {
                "S3Config": {
                    "Path": s3_OBJECT_KEY, 
                    "RoleArn": FORECAST_ROLE_ARN
                    }
                },
            TimestampFormat = TIMESTAMP_FORMAT
        )
    except forecast_client.exceptions.ResourceAlreadyExistsException as e:
        logger.info("ResourceAlreadyExistsException: %s" % e)
    except Exception as e:
        logger.info("Exception: %s" % e)