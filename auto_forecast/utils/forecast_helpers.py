import boto3
# see boto3 documentation for more details on list objects: 
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/forecast.html
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/forecastquery.html#ForecastQueryService.Client.query_forecast

def get_dataset_import_jobs(dataset_arn = ""):
    """[returns all dataset import jobs in a dataset arn]
    
    Keyword Arguments:
        dataset_arn {str} -- [dataset arn] (default: {""})
    
    Returns:
        [list] -- [list of dataset import jobs]
    """
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
    """[returns the latest dataset import job arn]
    
    Keyword Arguments:
        dataset_arn {str} -- [dataset arn] (default: {""})
    
    Returns:
        [str] -- [latest dataset import job arn]
    """
    import_jobs = get_dataset_import_jobs(dataset_arn)
    latest_import_job = import_jobs[0]
    
    for i in range(1, len(import_jobs)):
        if import_jobs[i]["CreationTime"] > latest_import_job["CreationTime"]: 
            latest_import_job = import_jobs[i]
    
    return latest_import_job["DatasetImportJobArn"]


def get_predictor_jobs(dataset_group_arn = ""):
    """[returns all dataset import jobs in a dataset arn]
    
    Keyword Arguments:
        dataset_group_arn {str} -- [dataset group arn] (default: {""})
    
    Returns:
        [list] -- [list of predictor jobs]
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
    

def get_latest_predictor_job_arn(dataset_group_arn = ""):
    """[returns the latest predictor job arn]
    
    Keyword Arguments:
        dataset_group_arn {str} -- [dataset group arn] (default: {""})
    
    Returns:
        [str] -- [predictor job arn]
    """
    predictor_jobs = get_predictor_jobs(dataset_group_arn)
    latest_predictor_job = predictor_jobs[0]
    
    for i in range(1, len(predictor_jobs)):
        if predictor_jobs[i]["CreationTime"] > latest_predictor_job["CreationTime"]: 
            latest_predictor_job = predictor_jobs[i]
    
    return latest_predictor_job["PredictorArn"]


def get_active_predictor_jobs(dataset_group_arn = ""):
    """[returns all active dataset predictor jobs]
    
    Keyword Arguments:
        dataset_group_arn {str} -- [dataset group arn] (default: {""})
    
    Returns:
        [list] -- [list of active predictor jobs]
    """
    predictor_jobs = get_predictor_jobs(dataset_group_arn)
    
    for i in range(0, len(predictor_jobs)):
        if predictor_jobs[i]["Status"] != "ACTIVE": 
            predictor_jobs.pop(i)
            
    return predictor_jobs


def get_latest_active_predictor_job_arn(dataset_group_arn = ""):
    """[returns the latest active predictor job arn]
    
    Keyword Arguments:
        dataset_group_arn {str} -- [dataset group arn] (default: {""})
    
    Returns:
        [str] -- [latest active predictor job arn]
    """
    actv_predictors = get_active_predictor_jobs(dataset_group_arn)
    latest_predictor_job = actv_predictors[0]
    
    for i in range(1, len(actv_predictors)):
        if actv_predictors[i]["CreationTime"] > latest_predictor_job["CreationTime"]: 
            latest_predictor_job = actv_predictors[i]
    
    return latest_predictor_job["PredictorArn"]


def get_forecast_values(forcast_arn = "", item_filter_value = ""):
    """[retrieves the forecast values of an item based on specified forecast job]
    
    Keyword Arguments:
        forcast_arn {str} -- [forecast arn] (default: {""})
        item_filter_value {str} -- [item filter] (default: {""})
    
    Returns:
        [dict] -- [resulting forecast result]
    """
    forecast_query_client = boto3.client("forecastquery")
    response = forecast_query_client.query_forecast(
        ForecastArn = forcast_arn,
        Filters = {
            "item_id": item_filter_value
        }
    ) 
    
    return response  


def get_forecast_jobs(dataset_group_arn = ""):
    """[returns all forecast jobs]
    
    Keyword Arguments:
        dataset_group_arn {str} -- [dataset group arn] (default: {""})
    
    Returns:
        [list] -- [list of forecast jobs]
    """
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
    """[returns the latest forecast job arn]
    
    Keyword Arguments:
        dataset_group_arn {str} -- [dataset group arn] (default: {""})
    
    Returns:
        [str] -- [latest forecast job arn]
    """
    forecast_jobs = get_forecast_jobs(dataset_group_arn)
    latest_forecast_job = forecast_jobs[0]
    
    for i in range(1, len(forecast_jobs)):
        if forecast_jobs[i]["CreationTime"] > latest_forecast_job["CreationTime"]: 
            latest_forecast_job = forecast_jobs[i]
    
    return latest_forecast_job["ForecastArn"]
  

def get_active_forecast_jobs(dataset_group_arn = ""):
    """[returns all active forecast jobs]
    
    Keyword Arguments:
        dataset_group_arn {str} -- [dataset group arn] (default: {""})
    
    Returns:
        [list] -- [list of active forecast jobs]
    """
    forecast_jobs = get_forecast_jobs(dataset_group_arn)
    
    for i in range(0, len(forecast_jobs)):
        if forecast_jobs[i]["Status"] != "ACTIVE": 
            forecast_jobs.pop(i)
            
    return forecast_jobs


def get_latest_active_forecast_job_arn(dataset_group_arn = ""):
    """[returns latest active forecast job arn]
    
    Keyword Arguments:
        dataset_group_arn {str} -- [dataset group arn] (default: {""})
    
    Returns:
        [str] -- [latest active forecast job arn]
    """
    actv_forecasts = get_active_forecast_jobs(dataset_group_arn)
    latest_forecast_job = actv_forecasts[0]
    
    for i in range(1, len(actv_forecasts)):
        if actv_forecasts[i]["CreationTime"] > latest_forecast_job["CreationTime"]: 
            latest_forecast_job = actv_forecasts[i]
    
    return latest_forecast_job["ForecastArn"]