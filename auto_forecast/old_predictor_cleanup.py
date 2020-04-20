import boto3
from forecast_base import ForecastBase

class OldPredictorCleanup(ForecastBase):
    def __init__(self, context):
        super().__init__(context)

    def old_predictor_cleanup(self):
        try:
            predictor_jobs = self.get_predictor_jobs()
            # cleanup old predictors
            for i in range(0, len(predictor_jobs)):
                if predictor_jobs[i]["PredictorArn"] != self.get_predictor_arn():
                    self.forecast_client.delete_predictor(PredictorArn = predictor_jobs[i]["PredictorArn"])
                    self.logger.info("Initialised delete predictor job: {0}".format(predictor_jobs[i]["PredictorArn"]))
        except Exception as e:
            self.logger.error("Exception: {0}".format(e))

    @staticmethod
    def get_predictor_jobs(dataset_group_arn=""):
        """Returns all existing predictor jobs under the dataset group
        
        Keyword Arguments:
            dataset_group_arn {string} -- dataset group arn (default: {""})
        
        Returns:
            list -- list of predictor jobs and their details from boto3 API
        """
        forecast_client = boto3.client("forecast")
        response = forecast_client.list_predictors(
            Filters = [
                {
                    "Key": "DatasetGroupArn",
                    "Value": dataset_group_arn,
                    "Condition": "IS"
                }
            ]
        )
        
        return response["Predictors"]

    @staticmethod
    def get_latest_predictor_job_arn(dataset_group_arn=""):
        """Returns the latest predictor job arn
        
        Keyword Arguments:
            dataset_group_arn {string} -- dataset group arn (default: {""})
            
        Returns:
            string -- Latest predictor job arn
        """
        predictor_jobs = OldPredictorCleanup.get_predictor_jobs(dataset_group_arn)
        latest_predictor_job = predictor_jobs[0]
        
        for i in range(1, len(predictor_jobs)):
            if predictor_jobs[i]["CreationTime"] > latest_predictor_job["CreationTime"]: 
                latest_predictor_job = predictor_jobs[i]
        
        return latest_predictor_job["PredictorArn"]


def handler(event, context):
    # instantiate class
    oldPredictorCleanup = OldPredictorCleanup(context)
    # run function
    oldPredictorCleanup.old_predictor_cleanup()
