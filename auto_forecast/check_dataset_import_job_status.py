from forecast_base import ForecastBase

class CheckDatasetImportJobStatus(ForecastBase):
    def __init__(self, context):
        super().__init__(context)

    def check_dataset_import_job_status(self):
        import_job_status = self.get_dataset_import_status(dataset_import_job_arn=self.get_dataset_import_job_arn()) 
        self.ActionBasedOnStatus(status=import_job_status, resource_arn=self.get_dataset_import_job_arn())
        
    def get_dataset_import_status(self, dataset_import_job_arn=""):
        """[returns the status of the dataset import job]
        
        Keyword Arguments:
            dataset_import_job_arn {str} -- [resource arn] (default: {""})
        
        Returns:
            [str] -- [the current status of the import job]
        """
        response = self.forecast_client.describe_dataset_import_job(DatasetImportJobArn=dataset_import_job_arn)
        return response["Status"]
        
        
def handler(event, context):
    # instantiate class
    checkDatasetImportJobStatus = CheckDatasetImportJobStatus(context)
    # run function
    checkDatasetImportJobStatus.check_dataset_import_job_status()
    
    
