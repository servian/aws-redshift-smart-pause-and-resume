# Redshift Smart Pause and Resume

![Language](https://img.shields.io/github/languages/top/servian/aws-auto-remediate.svg) [![serverless](http://public.serverless.com/badges/v3.svg)](http://www.serverless.com)

Open source tool to automatically pause and resume Redshift (single and multi-node) clusters using AWS Lambda, CloudWatch Metrics and Events, Amazon Forecast and Step Functions.

## About

### Resuming and Pausing Redshift using Cluster CPU utilisation Metrics from CloudWatch

CPU utilisation data from an existing Redshift data warehouse is scraped from Cloudwatch metrics. In particular, the metric is the average CPU utilisation at 15 minute intervals by default (value is configurable: recommended values are 5, 15, 30, 60 minute intervals). The data scraped is then used to train an Amazon forecast model, and the resulting forecast predictions are used to determine when to pausea and resume a Redshift data warehouse cluster. 

A threshold value is set and used determine when to pause and resume a Redshift cluster. To illustrate, if a threshold value of 5% (i.e., 5% CPU utilisation) is set, the Reshift cluster will be scheduled to resume around a timestamp when CPU utilisation is forecasted to be over the threshold value. On the other hand, given the same threshold value, a Reshift cluster will be scheduled pause around a timestamp when CPU utilisation is forecasted to be under the threshold value. 

An example of this is showcased below. For this example, given the forecasted CPU utilisation values:

* The Redshift cluster will be scheduled to resume on 7:45 
* The Redshift cluster will be scheduled to pause on 21:15 
* A buffer of 30 minutes is subtracted and added from the actual timestamps observed to give ample time for the Redshift cluster to resume and pause, respectively.

![schedule-example](images/schedule-example.svg)

### AWS Serverless Architecture 

This tool in a nutshell consists to 2 step functions: (1) the [Train Forecast Model Step Function](#train-forecast-model-step-function) and (2) the [Generate Forecasts Step Function](#generate-forecasts-step-function). Both of these step functions are executed using Lambda functions, and these Lambda functions are triggered with scheduled Cloudwatch Events. Events are scheduled based on the timezone specified when deploying the tool. 

![serverless-architecture](images/serverless-architecture.svg)

### Train Forecast Model Step Function

The following step function consists of a number of steps aimed to produce an Amazon forecast model predicting Redshift CPU utilisation. This step function can be scheduled to run more frequently depending on how often Redshift utilisation activity pattern changes. If AutoML is enabled the most appropriate forecast model will be fitted to the provided dataset. By default this step function is scheduled to run on the first day of each month at 9:00 (Time based on the provided timezone). 

### Generate Forecasts Step Function

The following step function consists of a number of steps aimed to produce Amazon forecast predictions using the resulting model trained from the [Train Forecast Model Step Function](#train-forecast-model-step-function). This step function is scheduled to run daily. In particular, Redshift metrics data from the previous day will be scraped and used alongside existing data to generate Redshift CPU utilisation forecasts for the following (current) day. Forecasts are then used to determine when to pause and resume the Redshift cluster. By default this step function is scheduled to run everyday at 12:05 midnight (Time is based on the provided timezone). 

## Setup
 
### Deploy 

1. Install Serverless Framework

```bash
npm install serverless
```

2. Install AWS CLI 

```bash
pip3 install awscli 
```

Configure AWS CLI following instructions found [here](#https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html#cli-quick-configuration). Ensure that the user configured has the appropriate IAM permissions to create Lambda Functions, S3 Buckets, IAM Roles, Step functions, Amazon Forecast resources and CloudFormation Stacks.

3. Install Redshift Smart Pause and Resume

```bash
serverless create --template-url https://github.com/servian/aws-redshift-smart-pause-and-resume --path aws-redshift-smart-pause-and-resume
```

4. Change to Redshift Smart Pause and Resume directory

```bash
cd aws-redshift-smart-pause-and-resume 
```

5. Install Serverless Plugins

```bash
serverless plugin install --name serverless-python-requirements
serverless plugin install --name serverless-iam-roles-per-function
serverless plugin install --name serverless-pseudo-parameters
serverless plugin install --name serverless-local-schedule
```

6. Deploy service to AWS Account. The option `redshiftclusterid` is required and need to be specified upon deploying the tool. (See [Deployment Options](#deployment-options) below for more details on the other options provided).

* **`NOTE:`** If deploying the tool to schedule another Redshift cluster, ensure that a different value is set for the option `servicename`. The value for this option is set to `smart-sched` by default. Possible value is `smart-sched-01`.

```bash
serverless deploy \
 [--region <AWS region>] \
 [--aws-profile <AWS CLI profile>] \
 [--redshiftclusterid <AWS redshift cluster id>]
 [--stage <deployment environment>] \
 [--servicename <tool service/stack name>] # default value is smart-sched
```

7. After deploying the tool, run the following command to initially scrape for data and train the forecast model. (See [Scraping and Training Forecast Model After Deployment](#Scraping-and-Training-Forecast-Model-After-Deployment) below for more details on the options provided).

* **`NOTE:`** When deploying the tool, if a value for the option `servicename` was specified and if this value is different from the default value `smart-sched`, be sure to provide the exact same value for `cfnstackname`. 

```bash
python3 local_scrape_and_train.py 
[--awsprofile <value>] \
[--numdaystoscrape <value>] \
[--cfnstackname  <value>] \ # value must be consitent with servicename from Deploy Step 6
[--stage  <value>] # value must be consistent with stage from Deploy Step 6
```

### Update

1. Remove existing library

2. Install/recreate Redshift Smart Pause and Resume

```bash
serverless create --template-url https://github.com/servian/aws-redshift-smart-pause-and-resume --path aws-redshift-smart-pause-and-resume
```

3. Change to Redshift Smart Pause and Resume directory

```bash
cd aws-redshift-smart-pause-and-resume 
```

3. Redeploy service to AWS Account. The option `redshiftclusterid` is required and need to be specified upon deploying the tool. (See [Deployment Options](#deployment-options) below for more details on the other options provided).

* **`NOTE:`** If updating the tool to schedule another Redshift cluster, ensure that the value set for the option `servicename` is consistent to the value when the tool was first deployed. The value for this option is set to `smart-sched` by default. Possible value is `smart-sched-01`, if this is the value used when deploying the tool to another Redshift cluster. 

```bash
serverless deploy \
 [--region <AWS region>] \
 [--aws-profile <AWS CLI profile>] \
 [--redshiftclusterid <AWS redshift cluster id>]
 [--stage <deployment environment>] \
 [--servicename <tool service/stack name>] # default value is smart-sched
```

### Remove

1. Change to Redshift Smart Pause and Resume directory

```bash
cd aws-redshift-smart-pause-and-resume 
```

2. Remove Service from AWS Account

```bash
serverless remove \
 [--region <AWS region>] \
 [--aws-profile <AWS CLI profile>] \
 [--redshiftclusterid <AWS redshift cluster id>]
 [--stage <deployment environment>] \
 [--servicename <tool service/stack name>] # default value is smart-sched
```

## Deployment Options

```
serverless deploy \
[--aws-profile <value>] \
[--region <value>] \
[--stage <value>] \
[--redshiftclusterid <value>] \
[--servicename <value>] \
[--enableautoml <value>] \
[--algorithmarn <value>] \
[--timezone <value>] \
```

`--aws-profile` (string)

AWS Profile to deploy resources (default value: `default`)

`--region` (string)

AWS Region to deploy resources (default value: `ap-southeast-2`: Sydney Region)

`--stage` (string)

deployment environment suffix (default value: `dev`)

`--redshiftclusterid` (string: REQUIRED)

unique identifier of the redshift cluster to enable smart scheduling

`--servicename` (string)

unique identifier for the tool stack (default value: `smart-sched`)

`--enableautoml` (string)

possible values `ENABLED` or `DISABLED` (default: `DISABLED`)

`--algorithmarn` (string)

[Possible values](https://docs.aws.amazon.com/forecast/latest/dg/aws-forecast-choosing-recipes.html) (default: `arn:aws:forecast:::algorithm/ARIMA`)

`--timezone` (string)

[Possible values](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)  (default: `Australia/Melbourne`)

`--intervalminutes` (int)

granularity of average Redshift CPU utilisation to use throughout stack (default: `15`)

## Scraping Data and Training Amazon Forecast Model After Deployment

The following script scrapes Redshift CPU utilisation data, and uses it to initially train an Amazon Forecast Model after deploying the stack by executing the Train Forecast Model Step Function. 

```bash
python3 local_scrape_and_train.py \
[--awsprofile <value>] \
[--numdaystoscrape <value>] \
[--cfnstackname  <value>] \ # value must be consitent with servicename from Deploy Step 6
[--stage  <value>] # value must be consistent with stage from Deploy Step 6
```

`--awsprofile` (string)

AWS Profile to deploy resources (default value: `default`)

`--numdaystoscrape` (string)

number of days (from previous day) worth of Redshift CPU utilisation data to scrape (default value: `14`)

`--cfnstackname` (string)

cloudformation stack name (default value: `smart-sched`. Which is `service-name` in serverless.yml template)

`--stage` (string)

environment suffix (default value: `dev`) 

## References

[Automating your Amazon Forecast Workflow with Lambda, Step Functions and Cloudwatch Events Rule](https://aws.amazon.com/blogs/machine-learning/automating-your-amazon-forecast-workflow-with-lambda-step-functions-and-cloudwatch-events-rule/)
