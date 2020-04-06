import json
import boto3
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    DATASET_NAME = os.environ['DATASET_NAME'] 
    DATASET_DOMAIN = os.environ['DATASET_DOMAIN']
    DATASET_TYPE = os.environ['DATASET_TYPE'] 
    DATASET_FREQ = os.environ['DATASET_FREQ'] 

    try:
        forecast_client = boto3.client("forecast")
        forecast_client.create_dataset(
            DatasetName = DATASET_NAME,
            Domain = DATASET_DOMAIN,
            DatasetType = DATASET_TYPE,
            DataFrequency = DATASET_FREQ,
            Schema = {
                "Attributes": [
                    {
                        "AttributeName": "item_id",
                        "AttributeType": "string"
                    },
                    {
                        "AttributeName": "timestamp",
                        "AttributeType": "timestamp"
                    },
                    {
                        "AttributeName": "target_value",
                        "AttributeType": "float"
                    }
                ]
            }
        )
    except forecast_client.exceptions.ResourceAlreadyExistsException as e:
        logger.info("ResourceAlreadyExistsException: %s" % e)
    except Exception as e:
        logger.info("Exception: %s" % e)
        