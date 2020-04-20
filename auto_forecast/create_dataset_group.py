from forecast_base import ForecastBase

class CreateDatasetGroup(ForecastBase):
    def __init__(self, context):
        super().__init__(context)

    def create_dataset_group(self):
        try:
            self.forecast_client.create_dataset_group(
                DatasetGroupName=self.DATASET_GROUP_NAME,
                Domain=self.DATASET_DOMAIN,
                DatasetArns=[self.get_dataset_arn()])
            self.logger.info("Create dataset group successful: {0}".format(self.DATASET_GROUP_NAME))
        except self.forecast_client.exceptions.ResourceAlreadyExistsException as e:
            self.logger.error("ResourceAlreadyExistsException: {0}".format(e))
        except Exception as e:
            self.logger.error("Exception: {0}".format(e))


def handler(event, context):
    # instantiate class
    createDatasetGroup = CreateDatasetGroup(context)
    # run function
    createDatasetGroup.create_dataset_group()
