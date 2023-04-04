import datetime

class LogGroup:
    _LOG_GROUP_TO_PREFIX = {
        "/aws/apigateway/": "aws/apigateway",
        "/aws/rds/cluster/": "aws/rds",
        "/aws/cloudhsm/": "aws/cloudhsm",
        "aws-cloudtrail-logs-": "aws/cloudtrail",
        "/aws/codebuild/": "aws/codebuild",
        "/aws/connect/": "aws/connect",
        "/aws/elasticbeanstalk/": "aws/elasticbeanstalk",
        "/aws/ecs/": "aws/ecs",
        "/aws/eks/": "aws/eks",
        "/aws-glue/": "glue",
        "AWSIotLogsV2": "aws/iot",
        "/aws/lambda/": "aws/lambda",
        "/aws/macie/": "aws/macie",
        "/aws/amazonmq/broker/": "aws/amazonmq"
    }

    def __init__(self, path: str, custom_fields: dict, start_time: int, interval: int) -> None:
        self.path = path
        self.custom_fields = custom_fields
        self.namespace = self._get_namespace_by_path()
        self.latest_time = self._get_first_latest_time(start_time, interval)

    def _get_namespace_by_path(self) -> str:
        for key in self._LOG_GROUP_TO_PREFIX:
            if self.path.startswith(key):
                return self._LOG_GROUP_TO_PREFIX[key]
        return ''

    def _get_first_latest_time(self, start_time: int, interval: int) -> int:
        dt = datetime.datetime.fromtimestamp(start_time)
        minutes_ago = dt - datetime.timedelta(minutes=interval)
        unix_seconds = int(minutes_ago.timestamp())
        return unix_seconds