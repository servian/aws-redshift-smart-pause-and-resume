from forecast_base import ForecastBase

class CreateDataset(ForecastBase):
    def __init__(self, context):
        super().__init__(context)

    def create_dataset(self):
        try:
            self.forecast_client.create_dataset(
                DatasetName=self.DATASET_NAME,
                Domain=self.DATASET_DOMAIN,
                DatasetType=self.DATASET_TYPE,
                DataFrequency=self.DATASET_FREQ,
                Schema={
                    "Attributes": [
                        {
                            "AttributeName": "item_id",
                            "AttributeType": "string"
                        },
                        {
                            "AttributeName": "timestamp",
                            "AttributeType": "timestamp"
                        },
                        {
                            "AttributeName": "target_value",
                            "AttributeType": "float"
                        }
                    ]
                }
            )
            self.logger.info("Create dataset successful: {0}".format(self.DATASET_NAME))
        except self.forecast_client.exceptions.ResourceAlreadyExistsException as e:
            self.logger.error("ResourceAlreadyExistsException: {0}".format(e))
        except Exception as e:
            self.logger.error("Exception: {0}".format(e))


def handler(event, context):
    # instantiate class
    createDataset = CreateDataset(context)
    # run function
    createDataset.create_dataset()
