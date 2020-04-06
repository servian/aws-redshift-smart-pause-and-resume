import boto3
import datetime as dt
import logging 
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    # env vars
    BUCKET_NAME = os.environ["BUCKET_NAME"]
    REDSHIFT_CLUSTER_ID = os.environ["REDSHIFT_CLUSTER_ID"]
    
    try:
        utc_start_time = dt.datetime.now() - dt.timedelta(days = 1) # this is because now metrics are taken daily, get metrics data from previous day
        utc_start_time = dt.datetime(utc_start_time.year, utc_start_time.month, utc_start_time.day, 0, 0, 0, 0, tzinfo = dt.timezone.utc)
        utc_end_time = utc_start_time + dt.timedelta(days = 1) 
        utc_iter_time = utc_start_time
            
        csv_string = "Item_Id,Timestamp,AverageCPUUtilisation\n" # header, order of columns for amazon forecast
        while utc_iter_time < utc_end_time:
            metrics = get_redshift_metrics(start_time = utc_iter_time, end_time = utc_iter_time + dt.timedelta(minutes=5), redshift_cluster_id = REDSHIFT_CLUSTER_ID)
            
            if metrics["Datapoints"]:
                csv_string = csv_string + REDSHIFT_CLUSTER_ID + "," 
                csv_string = csv_string + metrics["Datapoints"][0]["Timestamp"].strftime("%Y-%m-%d %H:%M:%S") + ","
                csv_string = csv_string + str(metrics["Datapoints"][0]["Average"]) + "\n"
            else:
                csv_string = csv_string + REDSHIFT_CLUSTER_ID + ","
                csv_string = csv_string + utc_iter_time.strftime("%Y-%m-%d %H:%M:%S") + ","
                csv_string = csv_string + str(0) + "\n"
                    
            utc_iter_time = utc_iter_time + dt.timedelta(minutes = 5) # increment time by 5 minutes
            
        s3_client = boto3.client("s3")
        s3_client.put_object(Body = csv_string, Bucket = BUCKET_NAME, Key = "{0}.csv".format(utc_start_time.strftime("%Y-%m-%d-%H:%M:%S")))
    except Exception as e:
        logger.info("Exception: %s" % e)
    

def get_redshift_metrics(start_time = dt.datetime(2000, 1, 1, 1, 0, 0, 0), end_time = dt.datetime(2000, 1, 1, 1, 0, 0, 0), redshift_cluster_id = ""):
    cloudwatch_client = boto3.client("cloudwatch")
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
                    "Value": "Shared" 
                    # checking the utilisation for the entire cluster
                    # currently this assumes this is only for a redshift instance with 1 node
                    # for multiple nodes need to take average/some other metric across all nodes
                    # to do this retrieve NumberOfNodes from describe_clusters
                    # then iterate across all nodes via filter value Compute-N from 0, 1, ...
                } 
            ],
            StartTime = start_time,
            EndTime = end_time,
            Period = 300,  # can take this as an argument, for now set to 5 minutes
            Statistics = ["Average"]
        )
    
    return response