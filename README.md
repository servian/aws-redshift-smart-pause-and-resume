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

```bash
npm install serverless
```

```bash
serverless plugin install --name serverless-python-requirements
serverless plugin install --name serverless-iam-roles-per-function
serverless plugin install --name serverless-pseudo-parameters
```

```bash


## References

![Automating your Amazon Forecast Workflows](https://aws.amazon.com/blogs/machine-learning/automating-your-amazon-forecast-workflow-with-lambda-step-functions-and-cloudwatch-events-rule/)
