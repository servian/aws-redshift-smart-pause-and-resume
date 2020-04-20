from forecast_base import ForecastBase

class CheckForecastJobStatus(ForecastBase):
    def __init__(self, context):
        super().__init__(context)

    def check_forecast_job_status(self):
        forecast_status = self.get_forecast_status(forecast_arn=self.get_forecast_arn())
        self.ActionBasedOnStatus(status=forecast_status, resource_arn=self.get_forecast_arn())
        
    def get_forecast_status(self, forecast_arn=""):
        """Returns the status of the forecast job
        
        Keyword Arguments:
            forecast_arn {string} -- Forecast job ARN (default: {""})
        
        Returns:
            string -- The current status of the forecast job
        """
        response = self.forecast_client.describe_forecast(ForecastArn=forecast_arn)
        return response["Status"]    
        
def handler(event, context):
    # instantiate class
    checkForecastJobStatus = CheckForecastJobStatus(context)
    # run function
    checkForecastJobStatus.check_forecast_job_status()
    
    
