import logging
import os
import yaml

from .log_group import LogGroup

logger = logging.getLogger(__name__)


class PositionManager:
    FIELD_PATH = 'path'
    FIELD_NEXT_TOKEN = 'next_token'
    FIELD_LATEST_TIME = 'latest_time'

    def __init__(self, file_path):
        self._file_path = file_path

    def update_position_file(self, log_group):
        pos_file_yaml = self.get_pos_file_yaml()
        if pos_file_yaml is not None:
            pos_file_yaml = self._update_position_yaml(pos_file_yaml, log_group)
        else:
            logger.warning('Could not open position file, will create new one')
            self._create_new_position_file()
            pos_file_yaml = []
            pos_file_yaml = self._update_position_yaml(pos_file_yaml, log_group)
        with open(self._file_path, 'w') as pos_file:
            yaml.dump(pos_file_yaml, pos_file)

    def get_pos_file_yaml(self):
        pos_file_yaml = None
        if os.path.exists(self._file_path):
            with open(self._file_path, 'r') as pos_file:
                pos_file_yaml = yaml.safe_load(pos_file)
        return pos_file_yaml

    def _update_position_yaml(self, pos_file_yaml, log_group):
        for pos_log_group in pos_file_yaml:
            if log_group.path == pos_log_group[self.FIELD_PATH]:
                logger.debug(f'Log group {log_group.path} exists in file, loading details')
                pos_log_group[self.FIELD_NEXT_TOKEN] = log_group.next_token
                pos_log_group[self.FIELD_LATEST_TIME] = log_group.latest_time
                return pos_file_yaml
        pos_file_yaml.append({self.FIELD_PATH: log_group.path,
                              self.FIELD_NEXT_TOKEN: log_group.next_token,
                              self.FIELD_LATEST_TIME: log_group.latest_time})
        return pos_file_yaml

    def _create_new_position_file(self):
        with open(self._file_path, 'w'):
            logger.info(f'Created position file at: {self._file_path}')
