import logging
import yaml

from .log_group import LogGroup

logger = logging.getLogger(__name__)


class ConfigReader:
    KEY_LOG_GROUPS = 'log_groups'
    KEY_INTERVAL = 'collection_interval'
    KEY_LOG_GROUP_PATH = 'path'
    KEY_LOG_GROUP_CUSTOM_FIELDS = 'custom_fields'
    KEY_LOG_GROUP_REGION = 'aws_region'

    def __init__(self, config_file):
        with open(config_file, 'r') as config:
            self._config_data = yaml.safe_load(config)
            logger.debug(self._config_data)

    def get_log_groups(self, start_time, default_interval):
        log_groups = []
        if self.KEY_LOG_GROUPS not in self._config_data or len(self._config_data[self.KEY_LOG_GROUPS]) == 0:
            logger.error('No log groups in config')
            return None
        for lgd in self._config_data[self.KEY_LOG_GROUPS]:
            if self.KEY_LOG_GROUP_PATH not in lgd:
                logger.error(f'Field {self.KEY_LOG_GROUP_PATH} not specified for a log group')
                return None
            path = lgd[self.KEY_LOG_GROUP_PATH]
            custom_fields = None
            if self.KEY_LOG_GROUP_CUSTOM_FIELDS in lgd:
                logger.debug(f'Found custom fields for {path}')
                custom_fields = lgd[self.KEY_LOG_GROUP_CUSTOM_FIELDS]
            interval = self.get_time_interval()
            if interval == 0:
                interval = default_interval
            log_group = LogGroup(path, custom_fields, start_time, interval)
            log_groups.append(log_group)
        return log_groups

    def get_time_interval(self):
        time_interval = 0
        if self.KEY_INTERVAL in self._config_data:
            try:
                time_interval = int(self._config_data[self.KEY_INTERVAL])
            except ValueError:
                logger.warning(f'Could not parse field {self.KEY_INTERVAL}')
        return time_interval

    def get_aws_region(self):
        if self.KEY_LOG_GROUP_REGION in self._config_data:
            return self._config_data[self.KEY_LOG_GROUP_REGION]
        else:
            return ''


