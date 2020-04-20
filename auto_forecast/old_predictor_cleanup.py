from forecast_base import ForecastBase

class OldPredictorCleanup(ForecastBase):
    def __init__(self, context):
        super().__init__(context)

    def old_predictor_cleanup(self):
        try:
            predictor_jobs = self.get_predictor_jobs(dataset_group_arn=self.get_dataset_group_arn())
            # cleanup old predictors
            for i in range(0, len(predictor_jobs)):
                if predictor_jobs[i]["PredictorArn"] != self.get_predictor_arn():
                    self.forecast_client.delete_predictor(PredictorArn = predictor_jobs[i]["PredictorArn"])
                    self.logger.info("Initialised delete predictor job: {0}".format(predictor_jobs[i]["PredictorArn"]))
        except Exception as e:
            self.logger.error("Exception: {0}".format(e))

    def get_predictor_jobs(self, dataset_group_arn = ""):
        """[returns all dataset import jobs in a dataset arn]
        
        Keyword Arguments:
            dataset_group_arn {str} -- [dataset group arn] (default: {""})
        
        Returns:
            [list] -- [list of predictor jobs]
        """
        response = self.forecast_client.list_predictors(
            Filters = [
                {
                    "Key": "DatasetGroupArn",
                    "Value": dataset_group_arn,
                    "Condition": "IS"
                }
            ]
        )
        
        return response["Predictors"]


def handler(event, context):
    # instantiate class
    oldPredictorCleanup = OldPredictorCleanup(context)
    # run function
    oldPredictorCleanup.old_predictor_cleanup()
