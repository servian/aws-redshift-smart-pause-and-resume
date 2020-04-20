from forecast_base import ForecastBase

class OldDatasetImportCleanup(ForecastBase):
    def __init__(self, context):
        super().__init__(context)

    def old_dataset_import_cleanup(self):
        try:
            dataset_import_jobs =  self.get_dataset_import_jobs()
            # cleanup old predictors
            for i in range(0, len(dataset_import_jobs)):
                if dataset_import_jobs[i]["DatasetImportJobArn"] != self.get_dataset_import_job_arn():
                    self.forecast_client.delete_dataset_import_job(DatasetImportJobArn=dataset_import_jobs[i]["DatasetImportJobArn"])
                    self.logger.info("Initialised delete dataset import job: {0}".format(dataset_import_jobs[i]["DatasetImportJobArn"]))
        except Exception as e:
            self.logger.error("Exception: {0}".format(e))

    def get_dataset_import_jobs(self):
        """Returns all dataset import jobs in a dataset arn
        
        Returns:
            list -- List of dataset import jobs and their details from boto3 API
        """
        import_jobs = self.forecast_client.list_dataset_import_jobs(
            Filters = [
                {
                    "Key": "DatasetArn",
                    "Value": self.get_dataset_arn(),
                    "Condition": "IS"
                }
            ]
        )
        
        return import_jobs["DatasetImportJobs"]

def handler(event, context):
    # instantiate class
    oldDatasetImportCleanup = OldDatasetImportCleanup(context)
    # run function
    oldDatasetImportCleanup.old_dataset_import_cleanup()
