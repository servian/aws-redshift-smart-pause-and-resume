import json
import boto3
import logging 
import os

from utils.forecast_helpers import get_latest_dataset_import_job_arn
from utils.forecast_helpers import derive_dataset_arn
from utils.custom_exceptions import ResourceCreateInProgressException
from utils.custom_exceptions import ResourceInFailedStateException

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    DATASET_NAME = os.environ["DATASET_NAME"]
    latest_import_job_arn = get_latest_dataset_import_job_arn(dataset_arn = derive_dataset_arn(DATASET_NAME, lambda_function_arn = context.invoked_function_arn))
    
    import_status = get_dataset_import_status(resource_arn = latest_import_job_arn) 
    if import_status.find("ACTIVE") != -1: 
        logger.info("resource {0} has STATUS:ACTIVE".format(latest_import_job_arn))
    elif import_status.find("FAILED") != -1:
        logger.info("resource {0} has STATUS:FAILED".format(latest_import_job_arn))
        raise ResourceInFailedStateException
    elif import_status.find("CREATE") != -1:
        logger.info("resource {0} has STATUS:CREATE".format(latest_import_job_arn))
        raise ResourceCreateInProgressException
    
    # {
    #   "errorType": "ResourceCreateInProgressException",
    #   "stackTrace": [
    #     "  File \"/var/task/create_dataset_import_job.py\", line 26, in handler\n    raise ResourceCreateInProgressException\n"
    #   ]
    # }
    
    
def get_dataset_import_status(resource_arn = ""):
    forecast_client = boto3.client("forecast")
    response = forecast_client.describe_dataset_import_job(DatasetImportJobArn = resource_arn)
    
    return response["Status"]