from forecast_base import ForecastBase


class CreateForecastJob(ForecastBase):
    def __init__(self, context):
        super().__init__(context)

    def create_forecast_job(self):
        try:
            self.forecast_client.create_forecast(
                ForecastName=self.FORECAST_NAME,
                PredictorArn=self.get_predictor_arn(),
                ForecastTypes=["mean"]
            )
            self.logger.info("Initialised create forecast job successful: {0}".format(self.FORECAST_NAME))
        except self.forecast_client.exceptions.ResourceAlreadyExistsException as e:
            self.logger.error("ResourceAlreadyExistsException: {0}".format(e))
        except Exception as e:
            self.logger.error("Exception: {0}".format(e))


def handler(event, context):
    # instantiate class
    createForecastJob = CreateForecastJob(context)
    # run function
    createForecastJob.create_forecast_job()
