import os
import boto3
import logging
import datetime as dt
from dateutil import tz

from utils.custom_exceptions import ResourceInFailedStateException
from utils.custom_exceptions import ResourceCreateInProgressException

class ForecastBase:
    def __init__(self, context):
        # variables from env variables
        self.DATASET_DOMAIN = os.environ["DATASET_DOMAIN"]
        self.DATASET_TYPE = os.environ['DATASET_TYPE']

        self.REDSHIFT_CLUSTER_ID = os.environ["REDSHIFT_CLUSTER_ID"]
        self.TIMEZONE = os.environ["TIMEZONE"]
        self.INTERVAL_MINUTES = os.environ["INTERVAL_MINUTES"]
        self.ALGORITHM_ARN = os.environ["ALGORITHM_ARN"]
        self.ENABLE_AUTOML = os.environ["ENABLE_AUTOML"]
        self.BUCKET_NAME = os.environ["BUCKET_NAME"]
        self.FORECAST_ROLE_ARN = os.environ["FORECAST_ROLE_ARN"] # to allow access to s3 bucket 

        self.FORECAST_HORIZON = int(24*(60/int(self.INTERVAL_MINUTES))) # one days worth of forecats given the interval in minutes
        self.FORECAST_FREQ = "{0}min".format(self.INTERVAL_MINUTES) # format is e.g, 5min, 10min etc.
        self.DATASET_FREQ = "{0}min".format(self.INTERVAL_MINUTES) # format is e.g, 5min, 10min etc.

        self.CURRENT_LOCAL_TIMESTAMP = dt.datetime.now().astimezone(tz.gettz(self.TIMEZONE)).strftime("%Y%m%d")
        self.LAMBDA_FUNCTION_ARN = context.invoked_function_arn
        self.REGION = self.LAMBDA_FUNCTION_ARN.split(":")[3]
        self.ACCOUNT_ID = self.LAMBDA_FUNCTION_ARN.split(":")[4]

        self.DATASET_NAME = "sched_{0}_dataset".format(self.REDSHIFT_CLUSTER_ID.replace("-", "_")) # is based on the redshiftclusterid but instead of "-" uses "_"
        self.DATASET_GROUP_NAME = "sched_{0}_dataset_group_name".format(self.REDSHIFT_CLUSTER_ID.replace("-", "_"))
        self.DATASET_IMPORT_JOB_NAME = "{0}_import{1}".format(self.DATASET_NAME, self.CURRENT_LOCAL_TIMESTAMP)
        self.PREDICTOR_NAME = "{0}_predictor{1}".format(self.DATASET_NAME, self.CURRENT_LOCAL_TIMESTAMP)
        self.FORECAST_NAME = "{0}_forecast{1}".format(self.DATASET_NAME, self.CURRENT_LOCAL_TIMESTAMP)

        self.forecast_client = boto3.client("forecast")
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)

    def get_dataset_group_arn(self):
        """returns dataset group arn

        Returns:
            [str] -- [dataset group arn]
        """
        return "arn:aws:forecast:{0}:{1}:dataset-group/{2}".format(self.REGION, self.ACCOUNT_ID, self.DATASET_GROUP_NAME)

    def get_dataset_arn(self):
        """returns dataset arn

        Returns:
            [str] -- [dataset arn]
        """
        return "arn:aws:forecast:{0}:{1}:dataset/{2}".format(self.REGION, self.ACCOUNT_ID, self.DATASET_NAME)

    def get_dataset_import_job_arn(self):
        """returns dataset import job arn

        Returns:
            [str] -- [dataset import job arn]
        """
        return "arn:aws:forecast:{0}:{1}:dataset-import-job/{2}/{3}".format(self.REGION, self.ACCOUNT_ID, self.DATASET_NAME, self.DATASET_IMPORT_JOB_NAME)

    def get_predictor_arn(self):
        """returns dataset predictor job arn

        Returns:
            [str] -- [dataset predictor job arn]
        """
        return "arn:aws:forecast:{0}:{1}:predictor/{2}".format(self.REGION, self.ACCOUNT_ID, self.PREDICTOR_NAME)
    
    def get_forecast_arn(self):
        """returns dataset predictor job arn

        Returns:
            [str] -- [dataset predictor job arn]
        """
        return "arn:aws:forecast:{0}:{1}:forecast/{2}".format(self.REGION, self.ACCOUNT_ID, self.FORECAST_NAME)
    
    def ActionBasedOnStatus(self, status="", resource_arn=""):
        """Commence an action based on the status of a resource
        
        Keyword Arguments:
            status {str} -- [current resource status] (default: {""})
            resource_arn {str} -- [resource arn] (default: {""})
        
        Raises:
            ResourceInFailedStateException: [description]
            ResourceCreateInProgressException: [description]
        """
        if status.find("ACTIVE") != -1: 
            self.logger.info("Resource {0} has STATUS:ACTIVE".format(resource_arn))
        elif status.find("FAILED") != -1:
            self.logger.info("Resource {0} has STATUS:FAILED".format(resource_arn))
            raise ResourceInFailedStateException
        elif status.find("CREATE") != -1:
            self.logger.info("Resource {0} has STATUS:CREATE".format(resource_arn))
            raise ResourceCreateInProgressException
