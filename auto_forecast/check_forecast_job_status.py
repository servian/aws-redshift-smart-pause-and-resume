import json
import boto3
import logging 
import os

from utils.forecast_helpers import get_latest_forecast_job_arn
from utils.forecast_helpers import derive_dataset_group_arn
from utils.custom_exceptions import ResourceCreateInProgressException
from utils.custom_exceptions import ResourceInFailedStateException

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    DATASET_GROUP_NAME = os.environ["DATASET_GROUP_NAME"]
    latest_forecast_arn = get_latest_forecast_job_arn(dataset_group_arn = derive_dataset_group_arn(dataset_group_name = DATASET_GROUP_NAME, lambda_function_arn = context.invoked_function_arn))
    
    import_status = get_forecast_status(resource_arn = latest_forecast_arn) 
    if import_status.find("ACTIVE") != -1: 
        logger.info("resource {0} has STATUS:ACTIVE".format(latest_forecast_arn))
    elif import_status.find("FAILED") != -1:
        logger.info("resource {0} has STATUS:FAILED".format(latest_forecast_arn))
        raise ResourceInFailedStateException
    elif import_status.find("CREATE") != -1:
        logger.info("resource {0} has STATUS:CREATE".format(latest_forecast_arn))
        raise ResourceCreateInProgressException
    
    
def get_forecast_status(resource_arn = ""):
    forecast_client = boto3.client("forecast")
    response = forecast_client.describe_forecast(ForecastArn = resource_arn)
    
    return response["Status"]