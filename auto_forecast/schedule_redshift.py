import json
import boto3
import logging
import os
import datetime as dt 
from dateutil import tz

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
    TIMEZONE = os.environ["TIMEZONE"]
    DATASET_GROUP_ARN = derive_dataset_group_arn(dataset_group_name = DATASET_GROUP_NAME, lambda_function_arn = context.invoked_function_arn)
    
    current_ts_utc = dt.datetime.utcnow().replace(tzinfo = tz.gettz("UTC")) # current timestamp in utc
    resume_scheduled = False
    pause_scheduled = False
    
    try:
        forecasts = get_forecast_values(forcast_arn = get_latest_forecast_job_arn(DATASET_GROUP_ARN), item_filter_value = REDSHIFT_CLUSTER_ID)
        for item in forecasts["Forecast"]["Predictions"]["mean"]:
            item_ts_local = dt.datetime.strptime(item["Timestamp"], "%Y-%m-%dT%H:%M:%S")
            item_ts_local  = item_ts_local.astimezone(tz.gettz(TIMEZONE)) # add timezone to timestamp
            item_ts_utc = item_ts_local.astimezone(tz.gettz("UTC")) # convert time to UTC, use this to schedule
            
            if item["Value"] > THRESHOLD and resume_scheduled == False and item_ts_utc > current_ts_utc:
                resume_ts_utc = item_ts_local - dt.timedelta(minutes = 30) # resume redshift earlier
                schedule_event(event_name = RESUME_REDSHIFT_EVENT_NAME, target_arn = RESUME_LAMBDA_ARN, event_role_arn = CLOUDWATCH_EVENT_ROLE_ARN, minute = resume_ts_utc.minute, hour = resume_ts_utc.hour)
                resume_scheduled == True
            elif item["Value"] < THRESHOLD and pause_scheduled == False:
                if  resume_scheduled == True and item_ts_utc > resume_ts_utc + dt.timedelta(hours = 2): # assign when resume is scheduled and let redshift be resumed for at least 2 hours
                    pause_ts_utc = item_ts_local + dt.timedelta(minutes = 30) # pause reshift later
                    schedule_event(event_name = PAUSE_REDSHIFT_EVENT_NAME, target_arn = PAUSE_LAMBDA_ARN, event_role_arn = CLOUDWATCH_EVENT_ROLE_ARN, minute = pause_ts_utc.minute, hour = pause_ts_utc.hour)        
                    pause_scheduled == True
                elif resume_scheduled == False: # assign when resume is not scheduled. i.e., cluster was resumed manually
                    pause_ts_utc = item_ts_local + dt.timedelta(minutes = 30) # pause reshift later
                    schedule_event(event_name = PAUSE_REDSHIFT_EVENT_NAME, target_arn = PAUSE_LAMBDA_ARN, event_role_arn = CLOUDWATCH_EVENT_ROLE_ARN, minute = pause_ts_utc.minute, hour = pause_ts_utc.hour)        
                    pause_scheduled == True
    except Exception as e:
        logger.info("Exception: {0}".format(e))

    
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