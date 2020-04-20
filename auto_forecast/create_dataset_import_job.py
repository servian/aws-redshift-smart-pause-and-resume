from forecast_base import ForecastBase

class CreateDatasetImportJob(ForecastBase):
    def __init__(self, context):
        super().__init__(context)
        self.s3_OBJECT_KEY = "s3://{0}/".format(self.BUCKET_NAME)
        self.TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss" 

    def create_dataset_import_job(self):
        try:
            self.forecast_client.create_dataset_import_job(
                DatasetImportJobName = self.DATASET_IMPORT_JOB_NAME,
                DatasetArn = self.get_dataset_arn(),
                DataSource = {
                    "S3Config": {
                        "Path": self.s3_OBJECT_KEY, 
                        "RoleArn": self.FORECAST_ROLE_ARN
                        }
                    },
                TimestampFormat = self.TIMESTAMP_FORMAT
            )
            self.logger.info("Initialised create dataset import job successful: {0}".format(self.DATASET_IMPORT_JOB_NAME))
        except self.forecast_client.exceptions.ResourceAlreadyExistsException as e:
            self.logger.info("ResourceAlreadyExistsException: {0}".format(e))
        except Exception as e:
            self.logger.error("Exception: {0}".format(e))


def handler(event, context):
    # instantiate class
    createDatasetImportJob = CreateDatasetImportJob(context)
    # run function
    createDatasetImportJob.create_dataset_import_job()