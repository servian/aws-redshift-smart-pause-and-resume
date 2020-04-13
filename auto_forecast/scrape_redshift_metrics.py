import json
import boto3
import logging
import os
import datetime as dt 
from dateutil import tz

from utils.forecast_helpers import get_forecast_values
from utils.forecast_helpers import derive_dataset_group_arn
from utils.forecast_helpers import get_latest_forecast_job_arn

logger = logging.getLogger()
logger.setLevel(logging.INFO)
  
def handler(event, context):    
    BUCKET_NAME = os.environ["BUCKET_NAME"]
    REDSHIFT_CLUSTER_ID = os.environ["REDSHIFT_CLUSTER_ID"]
    INTERVAL_MINUTES = int(os.environ["INTERVAL_MINUTES"])
    TIMEZONE = os.environ["TIMEZONE"]
    
    try:
        dt_end_utc = dt.datetime.utcnow().replace(tzinfo=tz.gettz('UTC')) # current timestamp in utc
        dt_end_local = dt_end_utc.astimezone(tz.gettz(TIMEZONE)) # convert to current timestamp in specified timezone
        dt_end_local = dt.datetime(dt_end_local.year, dt_end_local.month, dt_end_local.day, 0, 0, 0, 0).replace(tzinfo=tz.gettz(TIMEZONE))
        
        dt_start_local = dt_end_local - dt.timedelta(days=1) # previous day 
        dt_iter_local = dt_start_local 
            
        csv_string = "Item_Id,Timestamp,AverageCPUUtilisation\n" # header, order of columns for amazon forecast
        while dt_iter_local < dt_end_local:    
            dt_iter_utc = dt_iter_local.astimezone(tz.gettz('UTC')) # get utc counterpart of current time
                
            metrics = get_redshift_metrics(start_time = dt_iter_utc, end_time = dt_iter_utc + dt.timedelta(minutes = INTERVAL_MINUTES), redshift_cluster_id = REDSHIFT_CLUSTER_ID, interval_minutes = INTERVAL_MINUTES)
            csv_string = csv_string + REDSHIFT_CLUSTER_ID + "," 
            csv_string = csv_string + dt_iter_local.strftime("%Y-%m-%d %H:%M:%S") + ","
            csv_string = csv_string + str(metrics) + "\n"
            
            dt_iter_local  = dt_iter_local + dt.timedelta(minutes = INTERVAL_MINUTES) # increment time INTERVAL_MINUTES
            
        s3_client = boto3.client("s3")
        s3_client.put_object(Body = csv_string, Bucket = BUCKET_NAME, Key = "{0}.csv".format(dt_start_local.strftime("%Y-%m-%d-%H:%M:%S-%Z%z")))                   
        logger.info("done with {0} data".format(dt_start_local.strftime("%Y-%m-%d-%H:%M:%S-%Z%z")))
    except Exception as e:
        print("Exception: {0}".format(e))

    
def get_redshift_metrics(start_time = dt.datetime(2000, 1, 1, 1, 0, 0, 0), end_time = dt.datetime(2000, 1, 1, 1, 0, 0, 0), redshift_cluster_id = "", interval_minutes = 0):
    redshift_client = boto3.client("redshift")
    num_nodes = redshift_client.describe_clusters(ClusterIdentifier = redshift_cluster_id)["Clusters"][0]["NumberOfNodes"]
        
    if num_nodes == 1:
        nodes = ["Shared"]
    else:
        nodes = ["Compute-{0}".format(i) for i in range(0, num_nodes)]
        nodes.append("Leader")
    
    node_avg_metrics = []
    cloudwatch_client = boto3.client("cloudwatch")
    for node in nodes:
        response = cloudwatch_client.get_metric_statistics(
                Namespace = "AWS/Redshift",
                MetricName = "CPUUtilization", 
                Dimensions = [
                    {
                        "Name": "ClusterIdentifier", 
                        "Value": redshift_cluster_id
                    },
                    {
                        "Name": "NodeID",
                        "Value": node
                    } 
                ],
                StartTime = start_time,
                EndTime = end_time,
                Period = interval_minutes * 60, 
                Statistics = ["Average"]
            )

        if response["Datapoints"]:
            node_avg_metrics.append(float(response["Datapoints"][0]["Average"]))
        else:
            node_avg_metrics.append(0)
    
    return sum(node_avg_metrics)/len(node_avg_metrics) # avg across nodes