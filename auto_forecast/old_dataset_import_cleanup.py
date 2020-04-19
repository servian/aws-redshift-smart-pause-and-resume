import json
import boto3
import os
import logging

from utils.forecast_helpers import derive_dataset_arn
from utils.forecast_helpers import get_latest_dataset_import_job_arn
from utils.forecast_helpers import get_dataset_import_jobs

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    DATASET_NAME = os.environ["DATASET_NAME"]    
    DATASET_ARN = derive_dataset_arn(dataset_name = DATASET_NAME, lambda_function_arn = context.invoked_function_arn)
    
    try:
        dataset_import_jobs =  get_dataset_import_jobs(dataset_arn = DATASET_ARN)
        latest_dataset_import_job_arn = get_latest_dataset_import_job_arn(dataset_arn = DATASET_ARN)
        
        # cleanup old predictors
        forecast_client = boto3.client("forecast")
        for i in range(0, len(dataset_import_jobs)):
            if dataset_import_jobs[i]["DatasetImportJobArn"] != latest_dataset_import_job_arn:
                forecast_client.delete_dataset_import_job(DatasetImportJobArn = dataset_import_jobs[i]["DatasetImportJobArn"])
                logger.info("initialised delete dataset import job: {0}".format(dataset_import_jobs[i]["DatasetImportJobArn"]))
                # TODO: might need to catch the following exception ResourceInUseException in step functions in order to completely cleanup resource
    except Exception as e:
        logger.info("Exception: {0}".format(e))