import json
import boto3
import logging
import os
import datetime as dt 

from utils.forecast_helpers import get_forecast_values
from utils.forecast_helpers import derive_dataset_group_arn
from utils.forecast_helpers import get_latest_forecast_job_arn

logger = logging.getLogger()
logger.setLevel(logging.INFO)
  
def handler(event, context):    
    RESUME_LAMBDA_ARN = os.environ["RESUME_LAMBDA_ARN"]
    PAUSE_LAMBDA_ARN = os.environ["PAUSE_LAMBDA_ARN"]
    CLOUDWATCH_EVENT_ROLE_ARN = os.environ["CLOUDWATCH_EVENT_ROLE_ARN"]
    DATASET_GROUP_NAME = os.environ["DATASET_GROUP_NAME"]
    THRESHOLD = float(os.environ["THRESHOLD"])
    REDSHIFT_CLUSTER_ID = os.environ["REDSHIFT_CLUSTER_ID"]
    RESUME_REDSHIFT_EVENT_NAME = os.environ["RESUME_REDSHIFT_EVENT_NAME"]
    PAUSE_REDSHIFT_EVENT_NAME = os.environ["PAUSE_REDSHIFT_EVENT_NAME"]
    DATASET_GROUP_ARN = derive_dataset_group_arn(dataset_group_name = DATASET_GROUP_NAME, lambda_function_arn = context.invoked_function_arn)
    
    current_time = dt.datetime.now().time()
    resume_timestamp = current_time
    pause_timestamp = current_time
    
    try:
        # TODO: when no forecast is found above threshold then we just disable the schedules, cluster should remain paused
        forecasts = get_forecast_values(forcast_arn = get_latest_forecast_job_arn(DATASET_GROUP_ARN), item_filter_value = REDSHIFT_CLUSTER_ID)
        for item in forecasts["Forecast"]["Predictions"]["mean"]:
            item_timestamp = dt.datetime.strptime(item["Timestamp"], "%Y-%m-%dT%H:%M:%S").time()
            if item["Value"] > THRESHOLD and resume_timestamp == current_time and item_timestamp > current_time:
                resume_timestamp = item_timestamp
            elif item["Value"] < THRESHOLD and pause_timestamp == current_time and item_timestamp > resume_timestamp:
                pause_timestamp = item_timestamp
        
        # schedule resume
        schedule_event(event_name = RESUME_REDSHIFT_EVENT_NAME, target_arn = RESUME_LAMBDA_ARN, event_role_arn = CLOUDWATCH_EVENT_ROLE_ARN, minute = resume_timestamp.minute, hour = resume_timestamp.hour)
        # schedule pause
        schedule_event(event_name = PAUSE_REDSHIFT_EVENT_NAME, target_arn = PAUSE_LAMBDA_ARN, event_role_arn = CLOUDWATCH_EVENT_ROLE_ARN, minute = pause_timestamp.minute, hour = pause_timestamp.hour)
    except Exception as e:
        logger.info("Exception: %s" % e)

    
def schedule_event(event_name = "", target_arn = "", event_role_arn = "" , event_description = "", minute = 0, hour = 0):
    events_client = boto3.client("events")
    events_client.put_rule(
        Name = event_name,
        ScheduleExpression = "cron({0} {1} * * ? *)".format(minute, hour),
        State = "ENABLED",
        Description = event_description,
        RoleArn = event_role_arn
    )
    
    events_client.put_targets(
        Rule = event_name,
        Targets = [{
            "Id": event_name,
            "Arn": target_arn
        }]
    )    