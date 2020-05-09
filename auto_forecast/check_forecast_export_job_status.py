from forecast_base import ForecastBase

class CheckForecastExportJobStatus(ForecastBase):
    def __init__(self, context):
        super().__init__(context)

    def check_forecast_export_job_status(self):
        forecast_export_status = self.get_forecast_export_status(forecast_export_arn=self.get_forecast_export_job_arn()) 
        self.action_based_on_status(status=forecast_export_status, resource_arn=self.get_forecast_export_job_arn())
        
    def get_forecast_export_status(self, forecast_export_arn=""):
        """Returns the status of the predictor job
        
        Keyword Arguments:
            predictor_arn {string} -- Predictor ARN (default: {""})
        
        Returns:
            string -- The current status of the predictor job
        """
        response = self.forecast_client.describe_forecast_export_job(ForecastExportJobArn=forecast_export_arn)
        return response["Status"]    
        
def handler(event, context):
    # instantiate class
    checkForecastExportJobStatus = CheckForecastExportJobStatus(context)
    # run function
    checkForecastExportJobStatus.check_forecast_export_job_status()
    
    
