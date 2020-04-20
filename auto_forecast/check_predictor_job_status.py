from forecast_base import ForecastBase

class CheckPredictorJobStatus(ForecastBase):
    def __init__(self, context):
        super().__init__(context)

    def check_predictor_job_status(self):
        predictor_status = self.get_predictor_status(predictor_arn=self.get_predictor_arn()) 
        self.ActionBasedOnStatus(status=predictor_status, resource_arn=self.get_predictor_arn())
        
    def get_predictor_status(self, predictor_arn=""):
        """[returns the status of the predictor job]
        
        Keyword Arguments:
            predictor_arn {str} -- [resource arn] (default: {""})
        
        Returns:
            [str] -- [the current status of the predictor job]
        """
        response = self.forecast_client.describe_predictor(PredictorArn=predictor_arn)
        return response["Status"]    
        
def handler(event, context):
    # instantiate class
    checkPredictorJobStatus = CheckPredictorJobStatus(context)
    # run function
    checkPredictorJobStatus.check_predictor_job_status()
    
    
