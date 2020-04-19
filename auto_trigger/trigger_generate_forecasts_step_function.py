import json
import boto3
import os
import logging
import datetime as dt 

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]
    
    try:
        sfn_client = boto3.client("stepfunctions")
        sfn_client.start_execution(
            stateMachineArn = STATE_MACHINE_ARN,
            name = "generate_forecasts_run_" + dt.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        )
    except Exception as e:
        logger.info("Exception: {0}".format(e))
        
