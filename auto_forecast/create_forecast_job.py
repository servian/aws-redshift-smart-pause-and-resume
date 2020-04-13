import json
import boto3
import logging
import os
import datetime as dt

from utils.forecast_helpers import derive_dataset_group_arn
from utils.forecast_helpers import get_latest_active_predictor_job_arn

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    DATASET_NAME = os.environ["DATASET_NAME"]
    DATASET_GROUP_NAME = os.environ["DATASET_GROUP_NAME"]
    FORECAST_NAME = "{0}_forecast{1}".format(DATASET_NAME, dt.datetime.now().strftime("%Y%m%d"))
    DATASET_GROUP_ARN = derive_dataset_group_arn(dataset_group_name = DATASET_GROUP_NAME, lambda_function_arn = context.invoked_function_arn)
    
    try:
        forecast_client = boto3.client("forecast")
        PREDICTOR_ARN = get_latest_active_predictor_job_arn(DATASET_GROUP_ARN)
        
        forecast_client.create_forecast(
            ForecastName = FORECAST_NAME,
            PredictorArn = PREDICTOR_ARN,
            ForecastTypes = ["mean"]
        )
    except forecast_client.exceptions.ResourceAlreadyExistsException as e:        
        logger.info("ResourceAlreadyExistsException: {0}".format(e))
    except Exception as e:
        logger.info("Exception: {0}".format(e))