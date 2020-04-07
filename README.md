# Redshift Smart Resume and Pause (!!WIP)

![Language](https://img.shields.io/github/languages/top/servian/aws-auto-remediate.svg) [![serverless](http://public.serverless.com/badges/v3.svg)](http://www.serverless.com)

Open source application to automatically resume and pause Redshift using AWS Lambda, CloudWatch Metrics and Events, Amazon Forecast and Step Functions.

## About

### Resuming and Pausing Redshift using Cluster CPU Utilisation Metrics from CloudWatch

CPU Utilisation data from an existing Redshift data warehouse is scraped from Cloudwatch metrics. In particular, the metric is the average CPU utilisation at 5 minute intervals. The data scraped is then used to train an Amazon Forecast model, and the resulting forecast predictions are used to determine when to resume and pause a Redshift data warehouse. 

### Forecast Model Training Step Function

The following step function consists of a number of steps to produce an Amazon Forecast Predictor Model predicting Redshift CPU Utilisation. If AutoML is enabled this step function can be scheduled to run periodically. This step function can be scheduled to run more frequently depending on how often Redshift Utilisation activity pattern changes. When activity pattern changes AutoML will determine the most appropriate forecast model for the current set of data. 

![model-train-step-function](images/model-train-step-function.svg)

### Forecast Model Prediction Step Function

The following step function consists of a number of steps to produce Amazon Forecast Predictions using the resulting model trained from the above mentioned step function. This step function can be scheduled daily. In particular, Redshift metrics data scraped the previous day can be used alongside existing data to generate redshift metric forecasts the for the following day. Forecasts are then used to determine when to resume and pause the Redshift cluster. 

![model-prediction-step-function](images/model-prediction-step-function.svg)


## Setup
 
### Deploy 

1. Install Servereless Framework
```bash
npm install serverless
```

2. Install AWS CLI 
```bash
pip3 install awscli 
```

3. Configure the AWS CLI following the instruction at Quickly Configuring the AWS CLI. Ensure that the user you're configuring has the appropriate IAM permissions to create Lambda Functions, S3 Buckets, IAM Roles, Step functions, Amazon Forecast resources and CloudFormation Stacks.

4. Install Redshift Smart Pause and Resume
```bash
Install serverless create --template-url https://github.com/servian/aws-redshift-smart-pause-and-resume --aws-redshift-smart-pause-and-resume
```

5. Change to Redshift Smart Pause and Resume directory
```bash
cd aws-redshift-smart-pause-and-resume 
```

6. Install Serverless Plugins
```bash
serverless plugin install --name serverless-python-requirements
serverless plugin install --name serverless-iam-roles-per-function
serverless plugin install --name serverless-pseudo-parameters
```

6. Deploy Service to AWS Account
```bash
serverless deploy [--region <AWS region>] [--aws-profile <AWS CLI profile>]
```

### Update

1. Install Redshift Smart Pause and Resume
```bash
Install serverless create --template-url https://github.com/servian/aws-redshift-smart-pause-and-resume --aws-redshift-smart-pause-and-resume
```

2. Change to Redshift Smart Pause and Resume directory
```bash
cd aws-redshift-smart-pause-and-resume 
```

3. Redeploy Service from AWS Account
```bash
serverless deploy [--region <AWS region>] [--aws-profile <AWS CLI profile>]
```

### Remove

1. Change to Redshift Smart Pause and Resume directory
```bash
cd aws-redshift-smart-pause-and-resume 
```

2. Remove Service from AWS Account
```bash
serverless remove [--region <AWS region>] [--aws-profile <AWS CLI profile>]
```


## References

![Automating your Amazon Forecast Workflow with Lambda Step Functions and Cloudwatch Events Rule](https://aws.amazon.com/blogs/machine-learning/automating-your-amazon-forecast-workflow-with-lambda-step-functions-and-cloudwatch-events-rule/)
