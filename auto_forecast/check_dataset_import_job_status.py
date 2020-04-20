from forecast_base import ForecastBase

class CheckDatasetImportJobStatus(ForecastBase):
    def __init__(self, context):
        super().__init__(context)

    def check_dataset_import_job_status(self):
        import_job_status = self.get_dataset_import_status(dataset_import_job_arn=self.get_dataset_import_job_arn()) 
        self.ActionBasedOnStatus(status=import_job_status, resource_arn=self.get_dataset_import_job_arn())
        
    def get_dataset_import_status(self, dataset_import_job_arn=""):
        """Returns the status of the dataset import job
        
        Keyword Arguments:
            dataset_import_job_arn {string} -- Dataset Import Job ARN (default: {""})
        
        Returns:
            string -- The current status of the dataset import job
        """
        response = self.forecast_client.describe_dataset_import_job(DatasetImportJobArn=dataset_import_job_arn)
        return response["Status"]
        
        
def handler(event, context):
    # instantiate class
    checkDatasetImportJobStatus = CheckDatasetImportJobStatus(context)
    # run function
    checkDatasetImportJobStatus.check_dataset_import_job_status()
    
    
