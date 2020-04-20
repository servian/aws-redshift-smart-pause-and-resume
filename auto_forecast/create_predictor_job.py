from forecast_base import ForecastBase


class CreatePredictorJob(ForecastBase):
    def __init__(self, context):
        super().__init__(context)
        self.ENABLE_AUTOML_BOOL = False

    def create_predictor_job(self):
        try:
            if self.ENABLE_AUTOML == "true":
                self.ALGORITHM_ARN = ""
                self.ENABLE_AUTOML_BOOL = True

            self.forecast_client.create_predictor(
                PredictorName=self.PREDICTOR_NAME,
                AlgorithmArn=self.ALGORITHM_ARN,
                ForecastHorizon=self.FORECAST_HORIZON,
                PerformAutoML=self.ENABLE_AUTOML_BOOL,
                PerformHPO=False,
                EvaluationParameters={
                    "NumberOfBacktestWindows": 1,
                    "BackTestWindowOffset": self.FORECAST_HORIZON
                },
                InputDataConfig={
                    "DatasetGroupArn": self.get_dataset_group_arn()
                },
                FeaturizationConfig={
                    "ForecastFrequency": self.FORECAST_FREQ
                }
            )
            self.logger.info("Initialised create predictor job successful: {0}".format(self.PREDICTOR_NAME))
        except self.forecast_client.exceptions.ResourceAlreadyExistsException as e:
            self.logger.error("ResourceAlreadyExistsException: {0}".format(e))
        except Exception as e:
            self.logger.error("Exception: {0}".format(e))


def handler(event, context):
    # instantiate class
    createPredictorJob = CreatePredictorJob(context)
    # run function
    createPredictorJob.create_predictor_job()
