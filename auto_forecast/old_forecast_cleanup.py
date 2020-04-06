import json
import boto3
import os
import logging

from utils.forecast_helpers import derive_dataset_group_arn
from utils.forecast_helpers import get_latest_active_forecast_job_arn
from utils.forecast_helpers import get_active_forecast_jobs

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    DATASET_GROUP_NAME = os.environ["DATASET_GROUP_NAME"]
    DATASET_GROUP_ARN = derive_dataset_group_arn(dataset_group_name = DATASET_GROUP_NAME, lambda_function_arn = context.invoked_function_arn)
    
    try:
        actv_forecasts = get_active_forecast_jobs(dataset_group_arn = DATASET_GROUP_ARN)
        latest_forecast_job_arn = get_latest_active_forecast_job_arn(dataset_group_arn = DATASET_GROUP_ARN)
        
        # cleanup old forecasts
        forecast_client = boto3.client("forecast")
        for i in range(0, len(actv_forecasts)):
            if actv_forecasts[i]["ForecastArn"] != latest_forecast_job_arn:
                forecast_client.delete_forecast(ForecastArn = actv_forecasts[i]["ForecastArn"])
                logger.info("initialised delete forecast job: {0}".format(actv_forecasts[i]["ForecastArn"]))
    except Exception as e:
        logger.info("Exception: %s" % e)
    