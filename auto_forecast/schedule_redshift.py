import os
import boto3
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
        
        self.current_ts_utc = dt.datetime.utcnow().replace(tzinfo=tz.gettz("UTC")) # current timestamp in utc
        self.current_ts_local = self.current_ts_utc.astimezone(tz.gettz(self.TIMEZONE)) # current timestamp in local time based on timezone
        self.resume_scheduled = False
        self.pause_scheduled = False
        self.resume_ts_utc = self.current_ts_utc # placeholder
        self.pause_ts_utc = self.current_ts_utc # placeholder
        
        self.events_client = boto3.client("events")
        self.forecast_query_client = boto3.client("forecastquery")

    def schedule_redshift(self):
        try:
            forecasts = self.get_forecast_values(forcast_arn=self.get_forecast_arn(), item_filter_value=self.REDSHIFT_CLUSTER_ID)
            
            self.schedule_resume(forecasts=forecasts["Forecast"]["Predictions"]["mean"]) # always schedule the resume first
            self.schedule_pause(forecasts=forecasts["Forecast"]["Predictions"]["mean"])  
            
        except Exception as e:
            self.logger.error("Exception: {0}".format(e))
            
    def schedule_resume(self, forecasts = []):
        """Schedule when to resume the redshift cluster with the forecasts passed
        
        Keyword Arguments:
            forecasts {list} -- results from a get_forecast_values api call (default: {[]})
        """
        for item in forecasts:
            item_ts_local = dt.datetime.strptime(item["Timestamp"], "%Y-%m-%dT%H:%M:%S")    # timestamp from forecast
            item_ts_local  = item_ts_local.replace(tzinfo=tz.gettz(self.TIMEZONE))          # add timezone to timestamp
            item_ts_utc = item_ts_local.astimezone(tz.gettz("UTC"))                         # convert time to UTC, use this to schedule
            ts_diff_utc = item_ts_utc - self.current_ts_utc                                 # schedule event after current timestamp
            
            if item["Value"] > self.THRESHOLD and self.resume_scheduled == False and ts_diff_utc.total_seconds() >= 0: 
                resume_ts_utc = item_ts_utc - dt.timedelta(minutes = 30)                    # resume redshift earlier
                self.resume_scheduled = True
        
        if self.resume_scheduled:
            self.schedule_event(event_name=self.RESUME_REDSHIFT_EVENT_NAME, target_arn=self.RESUME_LAMBDA_ARN, event_role_arn=self.CLOUDWATCH_EVENT_ROLE_ARN, minute=resume_ts_utc.minute, hour=resume_ts_utc.hour)
            self.logger.info("Scheduled {0} to resume on {1} UTC".format(self.REDSHIFT_CLUSTER_ID, resume_ts_utc.strftime("%Y-%m-%dT%H:%M:%S")))
        else:
            self.logger.info("No scheduled resume on {0}".format(self.REDSHIFT_CLUSTER_ID))
            self.disable_scheduled_event(event_name=self.RESUME_REDSHIFT_EVENT_NAME)
            self.logger.info("Event: {0} DISABLED".format(self.RESUME_REDSHIFT_EVENT_NAME))
            
    def schedule_pause(self, forecasts = []):
        """Schedule when to pause the redshift cluster with the forecasts passed. Depends on resume redshift schedule variables 
        
        Keyword Arguments:
            forecasts {list} -- results from a get_forecast_values api call (default: {[]})
        """
        for item in forecasts:
            item_ts_local = dt.datetime.strptime(item["Timestamp"], "%Y-%m-%dT%H:%M:%S")    # timestamp from forecast
            item_ts_local  = item_ts_local.replace(tzinfo=tz.gettz(self.TIMEZONE))          # add timezone to timestamp
            item_ts_utc = item_ts_local.astimezone(tz.gettz("UTC"))                         # convert time to UTC, use this to schedule
            ts_diff_utc = item_ts_utc - self.current_ts_utc                                 # schedule event after current timestamp

            if item["Value"] < self.THRESHOLD and self.pause_scheduled == False and ts_diff_utc.total_seconds() >= 0:
                ts_diff_utc = item_ts_utc - self.resume_ts_utc + dt.timedelta(hours = 2)    # schedule event after two hours of resuming redshift
                
                if  self.resume_scheduled == True and ts_diff_utc.total_seconds() >= 0:     # assign when resume is scheduled and let redshift be resumed for at least 2 hours
                    pause_ts_utc = item_ts_utc + dt.timedelta(minutes = 30)                 # pause reshift later
                    self.pause_scheduled = True
                    
                elif self.resume_scheduled == False:                                        # assign when resume is not scheduled. i.e., cluster was resumed manually
                    pause_ts_utc = item_ts_utc + dt.timedelta(minutes = 30)                 # pause reshift later
                    self.pause_scheduled = True
        
        if self.pause_scheduled:
            self.schedule_event(event_name=self.PAUSE_REDSHIFT_EVENT_NAME, target_arn=self.PAUSE_LAMBDA_ARN, event_role_arn=self.CLOUDWATCH_EVENT_ROLE_ARN, minute=pause_ts_utc.minute, hour=pause_ts_utc.hour)        
            self.logger.info("Scheduled {0} to pause on {1} UTC".format(self.REDSHIFT_CLUSTER_ID, pause_ts_utc.strftime("%Y-%m-%dT%H:%M:%S")))
        else:
            self.logger.info("No scheduled pause on {0}".format(self.REDSHIFT_CLUSTER_ID))
            self.disable_scheduled_event(event_name=self.PAUSE_REDSHIFT_EVENT_NAME)
            self.logger.info("Event: {0} DISABLED".format(self.PAUSE_REDSHIFT_EVENT_NAME))
    
    def get_forecast_values(self, forcast_arn = "", item_filter_value = ""):
        """Retrieves the forecast values of an item using the specified forecast job with the query_forecast api
        
        Keyword Arguments:
            forcast_arn {str} -- forecast arn (default: {""})
            item_filter_value {str} -- item filter (default: {""})
        
        Returns:
            dict -- Resulting query forecast result
        """
        response = self.forecast_query_client.query_forecast(
            ForecastArn = forcast_arn,
            Filters = {
                "item_id": item_filter_value
            }
        ) 
        
        return response  
            
    def schedule_event(self, event_name = "", target_arn = "", event_role_arn = "" , event_description = "", minute = 0, hour = 0):
        """Schedule a Cloudwatch event sometime within a day
        
        Keyword Arguments:
            event_name {str} -- name of Cloudwatch event (default: {""})
            target_arn {str} -- arn of resource to attach Cloudwatch event to (default: {""})
            event_role_arn {str} -- role arn to attach to the Cloudwatch event  (default: {""})
            event_description {str} -- Cloudwatch event description (default: {""})
            minute {int} -- Minute value of the schedule e.g., a schedule at 20:05 has minute=5 (default: {0})
            hour {int} -- Minute value of the schedule e.g., a schedule at 20:05 has hour=20 (default: {0})
        """
        self.events_client.put_rule(
            Name=event_name,
            ScheduleExpression="cron({0} {1} * * ? *)".format(minute, hour),
            State="ENABLED",
            Description=event_description,
            RoleArn=event_role_arn
        )
        
        self.events_client.put_targets(
            Rule=event_name,
            Targets=[{
                "Id": event_name,
                "Arn": target_arn
            }]
        )  
    
    def disable_scheduled_event(self, event_name = ""):
        """Disable a scheduled Cloudwatch event
        
        Keyword Arguments:
            event_name {string} -- Cloudwatch event name to disable (default: {""})
        """
        self.events_client.disable_rule(Name=event_name)
  
def handler(event, context):
    # instantiate class
    scheduleRedshift = ScheduleRedshift(context)
    # run function
    scheduleRedshift.schedule_redshift()