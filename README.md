# Cloudwatch Fetcher

With this project you can define a time interval to fetch logs from AWS Cloudwatch, and ship them to Logz.io.

## Prerequisites

Before using this tool, you'll need to make sure that you have AWS access keys with permissions to:
* `logs:FilterLogEvents`
* `sts:GetCallerIdentity`


## Getting Started

### Available deployment methods:
- [Docker deployment](#docker-deployment).
- [Helm deployment](https://github.com/logzio/logzio-helm/tree/master/charts/cloudwatch-fetcher)


### Docker deployment

#### 1. Pull docker image

```shell
docker pull logzio/cloudwatch-fetcher:latest
```

#### 2. Create a data volume directory

This directory will hold the configuration and the position file for the fetcher.
The position file will allow the fetcher to resume to the point it last fetched, in case the container was stopped.

```shell
mkdir logzio-cloudwatch-fetcher \
&& cd logzio-cloudwatch-fetcher
``` 

#### 3. Create a configuration file

In the directory you created in the previous step, create a configuration file and name it `config.yaml`.

| Field                      | Description                                                                                      | Required/Default |
|----------------------------|--------------------------------------------------------------------------------------------------|------------------|
| `log_groups`               | An array of log group configuration                                                              | **Required**     |
| `log_groups.path`          | The AWS Cloudwatch log group you want to tail                                                    | **Required**     |
| `log_groups.custom_fields` | Array of key-value pairs, for adding custom fields to the logs from the log group                | -                |
| `aws_region`               | The AWS region your log groups are in. **Note** that all log groups should be in the same region | **Required**     |
| `collection_interval`      | Interval **IN MINUTES** to fetch logs from Cloudwatch                                            | Default: `5`     |


##### Configuration example

**See this [config sample](https://github.com/logzio/cloudwatch-fetcher/blob/master/config.yaml) for example.**

#### 4. Run the docker container

```shell
 docker run --name logzio-cloudwatch-fetcher \
-e AWS_ACCESS_KEY_ID=<<AWS-ACCESS-KEY>> \
-e AWS_SECRET_ACCESS_KEY=<<AWS-SECRET-KEY>> \
-e LOGZIO_LOG_SHIPPING_TOKEN=<<LOGZIO-LOGS-SHIPPING-TOKEN>> \
-e LOGZIO_LISTENER=https://<<LOGZIO-LISTENER>>:8071 \
-v "$(pwd)":/logzio/src/shared \
logzio/cloudwatch-fetcher:latest
```

Replace the following:

| Parameter                        | Description                                                                                                                                     |
|----------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| `<<AWS-ACCESS-KEY>>`             | Your AWS access key                                                                                                                             |
| `<<AWS-SECRET-KEY>>`             | Your AWS secret key                                                                                                                             |
| `<<LOGZIO-LOGS-SHIPPING-TOKEN>>` | Your [Logz.io logs shipping token](https://app.logz.io/#/dashboard/settings/general)                                                            |
| `<<LOGZIO-LISTENER>>`            | Your logz.io [listener url](https://app.logz.io/#/dashboard/settings/manage-tokens/data-shipping?product=logs), for example: `listener.logz.io` |

#### 5. Check Logz.io for your logs

Give your logs some time to get from your system to ours, and then open [Logz.io](https://app.logz.io/).

**NOTE** that the logs will have the original timestamp from Cloudwatch, so when you're searching for them, make sure that you're viewing the relevant time frame.

### Stop docker container

When you stop the container, the code will run until completion of the iteration.

To make sure it will finish the iteration on time, please give it a grace period of 30 seconds when you run the docker stop command:

```shell
docker stop -t 30 logzio-cloudwatch-fetcher
```

### Position file

After every successful iteration of each log group, the latest time & next token we got from AWS are will be written to a file name `position.yaml`

You can find the file inside your mounted host directory that you created.

If you stopped the container, the file will allow the fetcher to continue from the exact place it stopped.


## Changelog

- **0.0.1**: Initial release.