import boto3
import os
import datetime as dt 
from dateutil import tz
from forecast_base import ForecastBase

class ScheduleRedshift(ForecastBase):
    def __init__(self, context):
        super().__init__(context)
        self.REDSHIFT_CLUSTER_ID = os.environ["REDSHIFT_CLUSTER_ID"]
        self.CLOUDWATCH_EVENT_ROLE_ARN = os.environ["CLOUDWATCH_EVENT_ROLE_ARN"]
        self.RESUME_LAMBDA_ARN = os.environ["RESUME_LAMBDA_ARN"]
        self.PAUSE_LAMBDA_ARN = os.environ["PAUSE_LAMBDA_ARN"]
        self.THRESHOLD = float(os.environ["THRESHOLD"])
        self.RESUME_REDSHIFT_EVENT_NAME = os.environ["RESUME_REDSHIFT_EVENT_NAME"]
        self.PAUSE_REDSHIFT_EVENT_NAME = os.environ["PAUSE_REDSHIFT_EVENT_NAME"]

    def schedule_redshift(self):
        current_ts_utc = dt.datetime.utcnow().replace(tzinfo=tz.gettz("UTC")) # current timestamp in utc
        resume_scheduled = False
        pause_scheduled = False
        
        try:
            forecasts = self.get_forecast_values(forcast_arn=self.get_forecast_arn(), item_filter_value=self.REDSHIFT_CLUSTER_ID)
            for item in forecasts["Forecast"]["Predictions"]["mean"]:
                item_ts_local = dt.datetime.strptime(item["Timestamp"], "%Y-%m-%dT%H:%M:%S")
                item_ts_local  = item_ts_local.astimezone(tz.gettz(self.TIMEZONE)) # add timezone to timestamp
                item_ts_utc = item_ts_local.astimezone(tz.gettz("UTC")) # convert time to UTC, use this to schedule
                
                if item["Value"] > self.THRESHOLD and resume_scheduled == False and item_ts_utc > current_ts_utc:
                    resume_ts_utc = item_ts_local - dt.timedelta(minutes = 30) # resume redshift earlier
                    self.schedule_event(event_name = self.RESUME_REDSHIFT_EVENT_NAME, target_arn = self.RESUME_LAMBDA_ARN, event_role_arn = self.CLOUDWATCH_EVENT_ROLE_ARN, minute = resume_ts_utc.minute, hour = resume_ts_utc.hour)
                    self.logger.info("Scheduled {0} to resume on {1} UTC".format(self.REDSHIFT_CLUSTER_ID, resume_ts_utc.strptime("%Y-%m-%dT%H:%M:%S")))
                    resume_scheduled == True
                    
                elif item["Value"] < self.THRESHOLD and pause_scheduled == False:
                    if  resume_scheduled == True and item_ts_utc > resume_ts_utc + dt.timedelta(hours = 2): # assign when resume is scheduled and let redshift be resumed for at least 2 hours
                        pause_ts_utc = item_ts_local + dt.timedelta(minutes = 30) # pause reshift later
                        self.schedule_event(event_name = self.PAUSE_REDSHIFT_EVENT_NAME, target_arn = self.PAUSE_LAMBDA_ARN, event_role_arn = self.CLOUDWATCH_EVENT_ROLE_ARN, minute = pause_ts_utc.minute, hour = pause_ts_utc.hour)        
                        self.logger.info("Scheduled {0} to pause on {1} UTC".format(self.REDSHIFT_CLUSTER_ID, pause_ts_utc.strptime("%Y-%m-%dT%H:%M:%S")))
                        pause_scheduled == True
                        
                    elif resume_scheduled == False: # assign when resume is not scheduled. i.e., cluster was resumed manually
                        pause_ts_utc = item_ts_local + dt.timedelta(minutes = 30) # pause reshift later
                        self.schedule_event(event_name = self.PAUSE_REDSHIFT_EVENT_NAME, target_arn = self.PAUSE_LAMBDA_ARN, event_role_arn = self.CLOUDWATCH_EVENT_ROLE_ARN, minute = pause_ts_utc.minute, hour = pause_ts_utc.hour)        
                        self.logger.info("Scheduled {0} to pause on {1} UTC".format(self.REDSHIFT_CLUSTER_ID, pause_ts_utc.strptime("%Y-%m-%dT%H:%M:%S")))
                        pause_scheduled == True
                        
                        
        except Exception as e:
            self.logger.error("Exception: {0}".format(e))
    
    def get_forecast_values(self, forcast_arn = "", item_filter_value = ""):
        """[retrieves the forecast values of an item based on specified forecast job]
        
        Keyword Arguments:
            forcast_arn {str} -- [forecast arn] (default: {""})
            item_filter_value {str} -- [item filter] (default: {""})
        
        Returns:
            [dict] -- [resulting forecast result]
        """
        forecast_query_client = boto3.client("forecastquery")
        response = forecast_query_client.query_forecast(
            ForecastArn = forcast_arn,
            Filters = {
                "item_id": item_filter_value
            }
        ) 
        
        return response  
            
    def schedule_event(self, event_name = "", target_arn = "", event_role_arn = "" , event_description = "", minute = 0, hour = 0):
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
  
def handler(event, context):
    # instantiate class
    scheduleRedshift = ScheduleRedshift(context)
    # run function
    scheduleRedshift.schedule_redshift()