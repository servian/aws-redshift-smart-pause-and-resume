import json
import boto3
import os
import logging

from utils.forecast_helpers import derive_dataset_group_arn
from utils.forecast_helpers import get_latest_active_predictor_job_arn
from utils.forecast_helpers import get_active_predictor_jobs

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    DATASET_GROUP_NAME = os.environ["DATASET_GROUP_NAME"]    
    DATASET_GROUP_ARN = derive_dataset_group_arn(dataset_group_name = DATASET_GROUP_NAME, lambda_function_arn = context.invoked_function_arn)
    
    try:
        actv_predictors = get_active_predictor_jobs(dataset_group_arn = DATASET_GROUP_ARN)
        latest_predictor_arn = get_latest_active_predictor_job_arn(dataset_group_arn = DATASET_GROUP_ARN)
        
        # cleanup old predictors
        forecast_client = boto3.client("forecast")
        for i in range(0, len(actv_predictors)):
            if actv_predictors[i]["PredictorArn"] != latest_predictor_arn:
                forecast_client.delete_predictor(PredictorArn = actv_predictors[i]["PredictorArn"])
                logger.info("initialised delete predictor job: {0}".format(actv_predictors[i]["PredictorArn"]))
    except Exception as e:
        logger.info("Exception: {0}".format(e))