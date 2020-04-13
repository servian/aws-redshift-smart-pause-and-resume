import json
import boto3
import logging 
import os

from utils.forecast_helpers import get_latest_predictor_job_arn
from utils.forecast_helpers import derive_dataset_group_arn
from utils.custom_exceptions import ResourceCreateInProgressException
from utils.custom_exceptions import ResourceInFailedStateException

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    DATASET_GROUP_NAME = os.environ["DATASET_GROUP_NAME"]
    latest_predictor_arn = get_latest_predictor_job_arn(dataset_group_arn = derive_dataset_group_arn(dataset_group_name = DATASET_GROUP_NAME, lambda_function_arn = context.invoked_function_arn))
    
    import_status = get_predictor_status(resource_arn = latest_predictor_arn) 
    if import_status.find("ACTIVE") != -1: 
        logger.info("resource {0} has STATUS:ACTIVE".format(latest_predictor_arn))
    elif import_status.find("FAILED") != -1:
        logger.info("resource {0} has STATUS:FAILED".format(latest_predictor_arn))
        raise ResourceInFailedStateException
    elif import_status.find("CREATE") != -1:
        logger.info("resource {0} has STATUS:CREATE".format(latest_predictor_arn))
        raise ResourceCreateInProgressException
    
    
def get_predictor_status(resource_arn = ""):
    forecast_client = boto3.client("forecast")
    response = forecast_client.describe_predictor(PredictorArn = resource_arn)
    
    return response["Status"]