import os
import boto3
import logging
import datetime as dt 
from dateutil import tz

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]
    TIMEZONE = os.environ["TIMEZONE"]
    
    try:
        sfn_client = boto3.client("stepfunctions")
        sfn_client.start_execution(
            stateMachineArn = STATE_MACHINE_ARN,
            name = "train_forecast_model_run_{0}".format(dt.datetime.now().astimezone(tz.gettz(TIMEZONE)).strftime("%Y-%m-%d-%H-%M-%S")) 
        )
    except Exception as e:
        logger.error("Exception: {0}".format(e))
        
