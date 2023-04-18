import logging
import os
import yaml

from .log_group import LogGroup

logger = logging.getLogger(__name__)


class PositionManager:
    FIELD_PATH = 'path'
    FIELD_NEXT_TOKEN = 'next_token'
    FIELD_LATEST_TIME = 'latest_time'
    _DEFAULT_RESET_POSITION_FILE = 'false'
    ENV_RESET_POSITION = 'RESET_POSITION_FILE'

    def __init__(self, file_path):
        self._file_path = file_path
        reset_str = os.getenv(self.ENV_RESET_POSITION, self._DEFAULT_RESET_POSITION_FILE)
        if reset_str.lower() == 'true':
            self._delete_position_file()

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

    def _create_new_position_file(self, log_groups=None):
        with open(self._file_path, 'w') as pos_file:
            if log_groups is not None:
                yaml.dump(log_groups, pos_file)
            else:
                logger.info(f'Created position file at: {self._file_path}')

    def sync_position_file(self, log_groups_config):
        current_log_groups = []
        pos_yaml = self.get_pos_file_yaml()
        if pos_yaml is not None:
            for log_group in pos_yaml:
                for lg_config in log_groups_config:
                    if log_group[self.FIELD_PATH] == lg_config.path:
                        logger.info(f'Found previous data for log group {lg_config.path}')
                        current_log_groups.append(log_group)
                        break
            if len(current_log_groups) < len(pos_yaml):
                # Some log groups were removed, we need to remove them from the position file
                logger.debug(
                    f'Position file has {len(pos_yaml)} log groups, but only {len(current_log_groups)} match the current '
                    f'configuration')
                logger.debug('Updating position file')
                self._create_new_position_file(current_log_groups)

    def _delete_position_file(self):
        try:
            if os.path.exists(self._file_path):
                os.remove(self._file_path)
                logger.info('Deleted current position file')
            else:
                logger.warning(f'Env var {self.ENV_RESET_POSITION} is set to true, but no position file exists on {self._file_path}')
        except Exception as e:
            logger.error(f'Something went wrong while trying to delete current position file at {self._file_path}: {e}')
