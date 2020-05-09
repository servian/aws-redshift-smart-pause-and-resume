import os
import boto3
import argparse
import datetime as dt
from dateutil import tz
# python3 local_scrape_and_train.py

def init_argparse():
    """
    Console arguments
    """
    parser = argparse.ArgumentParser(description="Script to add data to Metrics Bucket to train initial Amazon Forecase Model")
    parser.add_argument("--awsprofile", default="default", type=str, help="AWS profile. If empty [default profile will be used if present]")
    parser.add_argument("--numdaystoscrape", default=14, type=int, help="Number of days of Redshift metric data to scrape to train initial Amazon Forecast Model")
    parser.add_argument("--cfnstackname", default="smart-sched", help="Name of cloudformation stack")
    parser.add_argument("--stage", default="dev", help="Name of environment")
    return parser


def scrape_redshift_metrics(bucket_name="", redshift_cluster_id="", num_days=7, timezone="Australia/Melbourne", interval_minutes=0):
    """Retrieve Redshift CPU utilisation metric data for the past num_days. Note: following should be able to adjust to DST

    Keyword Arguments:
        bucket_name {str} -- bucket location to store metrics scraped (default: {""})
        redshift_cluster_id {str} -- redshift cluster to scrape metrics from (default: {""})
        num_days {int} -- number of days of data to scrape (default: {7})
        timezone {str} -- timezone (default: {"Australia/Melbourne"})
        interval_minutes {int} -- interval between timepoints to collect data (default: {0})
    """
    try:
        num_days_scrape = num_days  # or i could just use num_days

        dt_end_utc = dt.datetime.utcnow().replace(tzinfo=tz.gettz('UTC'))  # current timestamp in utc
        # convert to current timestamp in specified timezone
        dt_end_local = dt_end_utc.astimezone(tz.gettz(timezone))
        dt_end_local = dt.datetime(dt_end_local.year, dt_end_local.month, dt_end_local.day, 0, 0, 0, 0).replace(tzinfo=tz.gettz(timezone))

        while True:
            dt_start_local = dt_end_local - dt.timedelta(days=1)  # previous day
            dt_iter_local = dt_start_local

            # header, order of columns for amazon forecast
            csv_string = "Item_Id,Timestamp,AverageCPUUtilisation\n"
            while dt_iter_local < dt_end_local:
                # get utc counterpart of current time
                dt_iter_utc = dt_iter_local.astimezone(tz.gettz('UTC'))

                metrics = get_redshift_metrics(start_time=dt_iter_utc, end_time=dt_iter_utc + dt.timedelta(minutes=interval_minutes), redshift_cluster_id=redshift_cluster_id, interval_minutes=interval_minutes)
                csv_string = csv_string + redshift_cluster_id + ","
                csv_string = csv_string + dt_iter_local.strftime("%Y-%m-%d %H:%M:%S") + ","
                csv_string = csv_string + str(metrics) + "\n"
                # csv_string = csv_string + metrics["Datapoints"][0]["Timestamp"].strftime("%Y-%m-%d %H:%M:%S") + "," # utc timestamp

                # increment by time interval_minutes
                dt_iter_local = dt_iter_local + dt.timedelta(minutes=interval_minutes)

            s3_client = boto3.client("s3")
            s3_client.put_object(Body=csv_string, Bucket=bucket_name, Key="{0}.csv".format(dt_start_local.strftime("%Y-%m-%d-%H:%M:%S-%Z%z")))

            if not num_days_scrape > 0:
                break

            num_days_scrape = num_days_scrape - 1
            # reset values
            dt_end_local = dt_end_local - dt.timedelta(days=1)
            csv_string = ""

            print("done with {0} data".format(dt_start_local.strftime("%Y-%m-%d-%H:%M:%S-%Z%z")))
    except Exception as e:
        print("Exception: {0}".format(e))


def get_redshift_metrics(start_time=dt.datetime(2000, 1, 1, 1, 0, 0, 0), end_time=dt.datetime(2000, 1, 1, 1, 0, 0, 0), redshift_cluster_id="", interval_minutes=0):
    """Scrape redshift metrics using get_metric_statistics api call from specified start_time to end_time. \
       Usage: it must be ensured that the duration interval between start_time and end_time is equal to the value provided for interval_minutes
    
    Keyword Arguments:
        start_time {datetime obj} -- start time to collect redshift metrics (default: {dt.datetime(2000, 1, 1, 1, 0, 0, 0)})
        end_time {datetime obj} -- end time to collect redshift metrics (default: {dt.datetime(2000, 1, 1, 1, 0, 0, 0)})
        redshift_cluster_id {string} -- redshift cluster id to obtain metrics from (default: {""})
        interval_minutes {int} -- interval between timestamps to aggragate metrics to (default: {0})
    
    Returns:
        float -- Average redshift CPU utilisation from start_time to end_time obtained every interval_minutes
    """
    time_diff = end_time - start_time
    if time_diff.total_seconds() != interval_minutes * 60:
        raise Exception("start_time and end_time diff in seconds not equal to provided interval_minutes")
    
    cloudwatch_client = boto3.client("cloudwatch")
    response = cloudwatch_client.get_metric_statistics(
        Namespace="AWS/Redshift",
        MetricName="CPUUtilization",
        Dimensions=[
            {
                "Name": "ClusterIdentifier",
                "Value": redshift_cluster_id
            }
        ],
        StartTime=start_time,
        EndTime=end_time,
        Period=interval_minutes * 60,
        Statistics=["Average"]
    )

    if response["Datapoints"]:
        return float(response["Datapoints"][0]["Average"])
    else:
        return float(0)


def get_cfn_resource_outputs(stack_output_list=[]):
    """Return outputs of a cloudformation stack as a dictionary

    Keyword Arguments:
        stack_output_list {list} -- cloudformation stack outputs as a list (default: {[]})

    Returns:
        dict -- Cloudformation stack outputs as a dictionary
    """
    return_val = {}

    for output in stack_output_list:
        return_val[output["OutputKey"]] = output["OutputValue"]

    return return_val


if __name__ == "__main__":
    # init
    parser = init_argparse()
    args = parser.parse_args()
    cfn_stack_name = "{0}-{1}".format(args.cfnstackname, args.stage)

    session = boto3.Session(profile_name=args.awsprofile)
    cfn_client = session.client("cloudformation")
    cfn_outputs = get_cfn_resource_outputs(stack_output_list=cfn_client.describe_stacks(StackName=cfn_stack_name)["Stacks"][0]["Outputs"])

    # scrape some initial redshift mettics
    scrape_redshift_metrics(bucket_name=cfn_outputs["MetricsBucketName"], redshift_cluster_id=cfn_outputs["RedshiftClusterId"], num_days=args.numdaystoscrape, timezone=cfn_outputs["Timezone"], interval_minutes=int(cfn_outputs["IntervalMinutes"]))
    print("Finished scraping {0} days worth of metrics data..".format(args.numdaystoscrape ))
    
    # train forecast model for the first time model
    sfn_client = session.client("stepfunctions")
    sfn_client.start_execution(stateMachineArn=cfn_outputs["TrainForecastModelStepFunction"], name="train_forecast_model_run_" + dt.datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S"))
    print("Executed 'Train Forecast Model Step Function'..")
    