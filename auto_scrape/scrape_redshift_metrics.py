import boto3
import os
import logging
import datetime as dt
from dateutil import tz

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# TODO: add capability to scrape for all incomplete data. e.g, when history has until 2020-04-06. if today is 2020-04-09, then need to scrape data for days 2020-04-07 and 2020-04-08
def handler(event, context):
    BUCKET_NAME = os.environ["BUCKET_NAME"]
    REDSHIFT_CLUSTER_ID = os.environ["REDSHIFT_CLUSTER_ID"]
    INTERVAL_MINUTES = int(os.environ["INTERVAL_MINUTES"])
    TIMEZONE = os.environ["TIMEZONE"]

    try:
        dt_end_utc = dt.datetime.utcnow().replace(tzinfo=tz.gettz('UTC'))  # current timestamp in utc
        # convert to current timestamp in specified timezone
        dt_end_local = dt_end_utc.astimezone(tz.gettz(TIMEZONE))
        dt_end_local = dt.datetime(dt_end_local.year, dt_end_local.month, dt_end_local.day, 0, 0, 0, 0).replace(tzinfo=tz.gettz(TIMEZONE))

        dt_start_local = dt_end_local - dt.timedelta(days=1)  # previous day
        dt_iter_local = dt_start_local

        # header, order of columns for amazon forecast
        csv_string = "Item_Id,Timestamp,AverageCPUUtilisation\n"
        while dt_iter_local < dt_end_local:
            # get utc counterpart of current time
            dt_iter_utc = dt_iter_local.astimezone(tz.gettz('UTC'))

            metrics = get_redshift_metrics(start_time=dt_iter_utc, end_time=dt_iter_utc + dt.timedelta(minutes=INTERVAL_MINUTES), redshift_cluster_id=REDSHIFT_CLUSTER_ID, interval_minutes=INTERVAL_MINUTES)
            csv_string = csv_string + REDSHIFT_CLUSTER_ID + ","
            csv_string = csv_string + \
                dt_iter_local.strftime("%Y-%m-%d %H:%M:%S") + ","
            csv_string = csv_string + str(metrics) + "\n"

            # increment time INTERVAL_MINUTES
            dt_iter_local = dt_iter_local + \
                dt.timedelta(minutes=INTERVAL_MINUTES)

        s3_client = boto3.client("s3")
        s3_client.put_object(Body=csv_string, Bucket=BUCKET_NAME, Key="{0}.csv".format(
            dt_start_local.strftime("%Y-%m-%d-%H:%M:%S-%Z%z")))
        logger.info("done with {0} data".format(dt_start_local.strftime("%Y-%m-%d-%H:%M:%S-%Z%z")))
    except Exception as e:
        logger.error("Exception: {0}".format(e))


def get_redshift_metrics(start_time=dt.datetime(2000, 1, 1, 1, 0, 0, 0), end_time=dt.datetime(2000, 1, 1, 1, 0, 0, 0), redshift_cluster_id="", interval_minutes=5):
    """Scrape redshift metrics using get_metric_statistics api call from specified start_time to end_time. \
       Usage: it must be ensured that duration interval between start_time and end_time is equal to the value provided for interval_minutes
    
    Keyword Arguments:
        start_time {datetime obj} -- start time to collect redshift metrics (default: {dt.datetime(2000, 1, 1, 1, 0, 0, 0)})
        end_time {datetime obj} -- end time to collect redshift metrics (default: {dt.datetime(2000, 1, 1, 1, 0, 0, 0)})
        redshift_cluster_id {string} -- redshift cluster id to obtain metrics from (default: {""})
        interval_minutes {int} -- interval between timestamps to aggragate metrics to (default: {0})
    
    Returns:
        float -- Average redshift CPU utilisation from start_time to end_time obtained every interval_minutes
    """
    redshift_client = boto3.client("redshift")
    num_nodes = redshift_client.describe_clusters(ClusterIdentifier=redshift_cluster_id)["Clusters"][0]["NumberOfNodes"]

    if num_nodes == 1:	
        nodes = ["Shared"]	
    else:	
        nodes = ["Compute-{0}".format(i) for i in range(0, num_nodes)]	
        nodes.append("Leader")	

    node_avg_metrics = []	
    cloudwatch_client = boto3.client("cloudwatch")
    for node in nodes:
        response = cloudwatch_client.get_metric_statistics(
            Namespace="AWS/Redshift",
            MetricName="CPUUtilization",	       
            Dimensions=[
                {	    
                    "Name": "ClusterIdentifier",
                    "Value": redshift_cluster_id
                },	        
                {	        
                    "Name": "NodeID",
                    "Value": node	 
                }	        
            ],	   
            StartTime=start_time,	
            EndTime=end_time,	
            Period=interval_minutes * 60,	
            Statistics=["Average"]	
        )	

        if response["Datapoints"]:	
            node_avg_metrics.append(	
                float(response["Datapoints"][0]["Average"]))	
        else:	
            node_avg_metrics.append(0)	


    return sum(node_avg_metrics)/len(node_avg_metrics)  # avg across nodes