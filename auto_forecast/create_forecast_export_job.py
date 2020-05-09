from forecast_base import ForecastBase

class CreateForecastExportJob(ForecastBase):
    def __init__(self, context):
        super().__init__(context)
        self.s3_OBJECT_KEY = "s3://{0}/".format(self.FORECAST_EXPORT_BUCKET)
        self.TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss" 

    def create_dataset_export_job(self):
        try:
            self.forecast_client.create_forecast_export_job(
                ForecastExportJobName=self.FORECAST_EXPORT_NAME,
                ForecastArn=self.get_forecast_arn(),
                Destination={
                    "S3Config": {
                        "Path": self.s3_OBJECT_KEY,
                        "RoleArn": self.FORECAST_ROLE_ARN
                    }
                }
            )
            self.logger.info("Initialised create forecast export job successful: {0}".format(self.FORECAST_EXPORT_NAME))
        except self.forecast_client.exceptions.ResourceAlreadyExistsException as e:
            self.logger.info("ResourceAlreadyExistsException: {0}".format(e))
        except Exception as e:
            self.logger.error("Exception: {0}".format(e))


def handler(event, context):
    # instantiate class
    createForecastExportJob = CreateForecastExportJob(context)
    # run function
    createForecastExportJob.create_dataset_export_job()