import json
import boto3
import logging
import os

from utils.forecast_helpers import derive_dataset_arn

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    DATASET_GROUP_NAME = os.environ["DATASET_GROUP_NAME"] 
    DATASET_NAME = os.environ["DATASET_NAME"] 
    DATASET_DOMAIN = os.environ["DATASET_DOMAIN"] 
    DATASET_ARN = derive_dataset_arn(dataset_name = DATASET_NAME, lambda_function_arn = context.invoked_function_arn)
    
    try:
        forecast_client = boto3.client("forecast")
        
        forecast_client.create_dataset_group(
            DatasetGroupName = DATASET_GROUP_NAME, 
            Domain = DATASET_DOMAIN, 
            DatasetArns = [DATASET_ARN])
    except forecast_client.exceptions.ResourceAlreadyExistsException as e:
        logger.info("ResourceAlreadyExistsException: %s" % e)
    except Exception as e:
        logger.info("Exception: %s" % e)