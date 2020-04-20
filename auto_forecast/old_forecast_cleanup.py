from forecast_base import ForecastBase

class OldForecastCleanup(ForecastBase):
    def __init__(self, context):
        super().__init__(context)

    def old_forecast_cleanup(self):
        try:
            forecast_jobs = self.get_forecast_jobs()
            # cleanup old forecasts
            for i in range(0, len(forecast_jobs)):
                if forecast_jobs[i]["ForecastArn"] != self.get_forecast_arn():
                    self.forecast_client.delete_forecast(ForecastArn = forecast_jobs[i]["ForecastArn"])
                    self.logger.info("Initialised delete forecast job: {0}".format(forecast_jobs[i]["ForecastArn"]))
        except Exception as e:
            self.logger.error("Exception: {0}".format(e))

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


def handler(event, context):
    # instantiate class
    oldForecastCleanup = OldForecastCleanup(context)
    # run function
    oldForecastCleanup.old_forecast_cleanup()
