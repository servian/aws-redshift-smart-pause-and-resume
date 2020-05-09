import boto3
from forecast_base import ForecastBase

class OldForecastCleanup(ForecastBase):
    def __init__(self, context):
        super().__init__(context)
        self.s3_client = boto3.client("s3")

    def old_forecast_cleanup(self):
        try:
            forecast_jobs = self.get_forecast_jobs()
            # cleanup old forecasts
            for i in range(0, len(forecast_jobs)):
                if forecast_jobs[i]["ForecastArn"] != self.get_forecast_arn():
                    self.delete_forecast_job(forecast_job_arn=forecast_jobs[i]["ForecastArn"])
                    
                    # cleanup associated forecast export jobs
                    forecast_export_jobs = self.get_forecast_export_jobs(forecast_arn=forecast_jobs[i]["ForecastArn"])
                    for i in range(0, len(forecast_export_jobs)):
                        self.delete_forecast_export_job(forecast_export_job_arn=forecast_export_jobs[i]["ForecastExportJobArn"])
                        
            # cleanup old forecast export job csv files in forecast export s3 bucket
            bucket_objects = self.s3_client.list_objects(Bucket=self.FORECAST_EXPORT_BUCKET)["Contents"]
            for i in range(0, len(bucket_objects)):
                if not (self.FORECAST_EXPORT_NAME in bucket_objects[i]["Key"]):
                    self.s3_client.delete_object(Bucket=self.FORECAST_EXPORT_BUCKET, Key=bucket_objects[i]["Key"])
                    self.logger.info("Deleted forecast export job csv file in {0}: {1}".format(self.FORECAST_EXPORT_BUCKET, bucket_objects[i]["Key"]))
        except Exception as e:
            self.logger.error("Exception: {0}".format(e))

    def delete_forecast_job(self, forecast_job_arn=""):
        """delete forecast job
        
        Keyword Arguments:
            forecast_job_arn {string} -- dataset forecast arn (default: {""})
        """
        try:
            self.forecast_client.delete_forecast(ForecastArn=forecast_job_arn)
            self.logger.info("Initialised delete forecast job: {0}".format(forecast_job_arn))
        except:
            pass
        
    def delete_forecast_export_job(self, forecast_export_job_arn=""):
        """delete forecast export job
        
        Keyword Arguments:
            forecast_export_job_arn {string} -- forecast export job arn (default: {""})
        """
        try:
            self.forecast_client.delete_forecast_export_job(ForecastExportJobArn=forecast_export_job_arn) 
            self.logger.info("Initialised delete forecast export job: {0}".format(forecast_export_job_arn))
        except:
            pass
        
    def get_forecast_jobs(self):
        """Returns all forecast jobs
        
        Returns:
            list -- List of forecast jobs and their details from boto3 API
        """
        response = self.forecast_client.list_forecasts(
            Filters = [
                {
                    "Key": "DatasetGroupArn",
                    "Value": self.get_dataset_group_arn(),
                    "Condition": "IS"
                }
            ]
        )
        
        return response["Forecasts"]
    
    def get_forecast_export_jobs(self, forecast_arn=""):
        """Returns all forecast export jobs
        
        Returns:
            list -- List of forecast export jobs and their details from boto3 API
        """
        response = self.forecast_client.list_forecast_export_jobs(
            Filters=[
                {
                    "Key": "ForecastArn",
                    "Value": forecast_arn,
                    "Condition": "IS"
                }
            ]
        )
        
        return response["ForecastExportJobs"]


def handler(event, context):
    # instantiate class
    oldForecastCleanup = OldForecastCleanup(context)
    # run function
    oldForecastCleanup.old_forecast_cleanup()
