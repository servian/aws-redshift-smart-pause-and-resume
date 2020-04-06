import boto3

def derive_dataset_group_arn(dataset_group_name = "", lambda_function_arn = ""):
    account_id = lambda_function_arn.split(":")[4]
    region = lambda_function_arn.split(":")[3]
    
    return "arn:aws:forecast:{0}:{1}:dataset-group/{2}".format(region, account_id, dataset_group_name)


def derive_dataset_arn(dataset_name = "", lambda_function_arn = ""):
    account_id = lambda_function_arn.split(":")[4]
    region = lambda_function_arn.split(":")[3]
    
    return "arn:aws:forecast:{0}:{1}:dataset/{2}".format(region, account_id, dataset_name)


def derive_dataset_import_job_arn(dataset_name = "", dataset_import_job_name = "", lambda_function_arn = ""):
    account_id = lambda_function_arn.split(":")[4]
    region = lambda_function_arn.split(":")[3]
    
    return "arn:aws:forecast:{0}:{1}:dataset-import-job/{2}/{3}".format(region, account_id, dataset_name, dataset_import_job_name) 


def derive_predictor_arn(predictor_name = "", lambda_function_arn = ""):
    account_id = lambda_function_arn.split(":")[4]
    region = lambda_function_arn.split(":")[3]
    
    return "arn:aws:forecast:{0}:{1}:predictor/{2}".format(region, account_id, predictor_name)


def get_dataset_import_jobs(dataset_arn = ""):
    forecast_client = boto3.client("forecast")
    import_jobs = forecast_client.list_dataset_import_jobs(
        Filters = [
            {
                "Key": "DatasetArn",
                "Value": dataset_arn,
                "Condition": "IS"
            }
        ]
    )
    
    return import_jobs["DatasetImportJobs"]


def get_latest_dataset_import_job_arn(dataset_arn = ""):
    import_jobs = get_dataset_import_jobs(dataset_arn)
    latest_import_job = import_jobs[0]
    
    for i in range(1, len(import_jobs)):
        if import_jobs[i]["CreationTime"] > latest_import_job["CreationTime"]: 
            latest_import_job = import_jobs[i]
    
    return latest_import_job["DatasetImportJobArn"]


def get_predictor_jobs(dataset_group_arn = ""):
    """returns a list of all predictor jobs regardless of STATUS"""
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
    

def get_latest_predictor_job_arn(dataset_group_arn = ""):
    """returns a the latest predictor job regardless of STATUS"""
    predictor_jobs = get_predictor_jobs(dataset_group_arn)
    latest_predictor_job = predictor_jobs[0]
    
    for i in range(1, len(predictor_jobs)):
        if predictor_jobs[i]["CreationTime"] > latest_predictor_job["CreationTime"]: 
            latest_predictor_job = predictor_jobs[i]
    
    return latest_predictor_job["PredictorArn"]


def get_active_predictor_jobs(dataset_group_arn = ""):
    """returns a list of all predictor jobs with STATUS=ACTIVE"""
    predictor_jobs = get_predictor_jobs(dataset_group_arn)
    
    for i in range(0, len(predictor_jobs)):
        if predictor_jobs[i]["Status"] != "ACTIVE": 
            predictor_jobs.pop(i)
            
    return predictor_jobs


def get_latest_active_predictor_job_arn(dataset_group_arn = ""):
    """returns a the latest predictor job with STATUS=ACTIVE"""
    actv_predictors = get_active_predictor_jobs(dataset_group_arn)
    latest_predictor_job = actv_predictors[0]
    
    for i in range(1, len(actv_predictors)):
        if actv_predictors[i]["CreationTime"] > latest_predictor_job["CreationTime"]: 
            latest_predictor_job = actv_predictors[i]
    
    return latest_predictor_job["PredictorArn"]


def get_forecast_values(forcast_arn = "", item_filter_value = ""):
    forecast_query_client = boto3.client("forecastquery")
    response = forecast_query_client.query_forecast(
        ForecastArn = forcast_arn,
        Filters = {
            "item_id": item_filter_value
        }
    ) 
    
    return response  


def get_forecast_jobs(dataset_group_arn = ""):
    """returns a list of all forecast jobs regardless of STATUS"""
    forecast_client = boto3.client("forecast")
    response = forecast_client.list_forecasts(
        Filters = [
            {
                "Key": "DatasetGroupArn",
                "Value": dataset_group_arn,
                "Condition": "IS"
            }
        ]
    )
    
    return response["Forecasts"]
    

def get_latest_forecast_job_arn(dataset_group_arn = ""):
    """returns a the latest forecast job regardless of STATUS"""
    forecast_jobs = get_forecast_jobs(dataset_group_arn)
    latest_forecast_job = forecast_jobs[0]
    
    for i in range(1, len(forecast_jobs)):
        if forecast_jobs[i]["CreationTime"] > latest_forecast_job["CreationTime"]: 
            latest_forecast_job = forecast_jobs[i]
    
    return latest_forecast_job["ForecastArn"]
  

def get_active_forecast_jobs(dataset_group_arn = ""):
    """returns a list of all forecast jobs with STATUS=ACTIVE"""
    forecast_jobs = get_forecast_jobs(dataset_group_arn)
    
    for i in range(0, len(forecast_jobs)):
        if forecast_jobs[i]["Status"] != "ACTIVE": 
            forecast_jobs.pop(i)
            
    return forecast_jobs


def get_latest_active_forecast_job_arn(dataset_group_arn = ""):
    """returns a the latest forecast job with STATUS=ACTIVE"""
    actv_forecasts = get_active_forecast_jobs(dataset_group_arn)
    latest_forecast_job = actv_forecasts[0]
    
    for i in range(1, len(actv_forecasts)):
        if actv_forecasts[i]["CreationTime"] > latest_forecast_job["CreationTime"]: 
            latest_forecast_job = actv_forecasts[i]
    
    return latest_forecast_job["ForecastArn"]