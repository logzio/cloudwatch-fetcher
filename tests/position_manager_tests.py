import logging
import unittest
import os
import yaml

from src.position_manager import PositionManager
from src.log_group import LogGroup


class PositionManagerTests(unittest.TestCase):
    NEW_POS_FILE = 'position.yaml'
    POS_FILE_EXISTS = 'fixture/position.yaml'

    def tearDown(self):
        position_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), self.NEW_POS_FILE)
        if os.path.exists(position_file):
            os.remove(position_file)

    def _get_pm(self, file_path):
        position_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), file_path)
        return PositionManager(position_file)

    def test_get_position_file_not_exist(self):
        pm = self._get_pm(self.NEW_POS_FILE)
        pos_file_yaml = pm.get_pos_file_yaml()
        self.assertIsNone(pos_file_yaml)

    def test_get_position_file(self):
        pm = self._get_pm(self.POS_FILE_EXISTS)
        pos_file_yaml = pm.get_pos_file_yaml()
        self.assertIsNotNone(pos_file_yaml)
        self.assertEqual(2, len(pos_file_yaml))
        for pos_data in pos_file_yaml:
            self.assertGreater(pos_data[pm.FIELD_LATEST_TIME], 0)
            self.assertGreater(len(pos_data[pm.FIELD_PATH]), 0)
            self.assertGreater(len(pos_data[pm.FIELD_NEXT_TOKEN]), 0)

    def test_update_position_file_no_file(self):
        log_group = LogGroup('my_path', None, 1681389974, 10)
        log_group.next_token = 'a-next-token'
        pm = self._get_pm(self.NEW_POS_FILE)
        self.assertTrue(not os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), self.NEW_POS_FILE)))
        pm.update_position_file(log_group)
        self.assertLogs('src.position_manager', logging.WARNING)
        self.assertTrue(os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), self.NEW_POS_FILE)))
        pos_yaml = pm.get_pos_file_yaml()
        self.assertEqual(1, len(pos_yaml))
        self.assertEqual(log_group.path, pos_yaml[0][pm.FIELD_PATH])
        self.assertEqual(log_group.latest_time, pos_yaml[0][pm.FIELD_LATEST_TIME])
        self.assertEqual(log_group.next_token, pos_yaml[0][pm.FIELD_NEXT_TOKEN])

    def test_update_position_file(self):
        pm = PositionManager(self.NEW_POS_FILE)
        pos_data = [{'latest_time': 1681389974, 'next_token': 'some-token-123', 'path': '/a/log/group'},
                    {'latest_time': 1681389974, 'next_token': 'another-token-456', 'path': 'some-other-log-group'}]
        position_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), self.NEW_POS_FILE)
        with open(position_file_path, 'w') as pf:
            yaml.dump(pos_data, pf)
        self.assertTrue(os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), self.NEW_POS_FILE)))
        with open(position_file_path, 'r') as pf:
            test_yaml = yaml.safe_load(pf)
            self.assertIsNotNone(test_yaml)
            self.assertEqual(2, len(test_yaml))
            for idx, pd in enumerate(test_yaml):
                self.assertEqual(pos_data[idx][pm.FIELD_PATH], test_yaml[idx][pm.FIELD_PATH])
                self.assertEqual(pos_data[idx][pm.FIELD_LATEST_TIME], test_yaml[idx][pm.FIELD_LATEST_TIME])
                self.assertEqual(pos_data[idx][pm.FIELD_NEXT_TOKEN], test_yaml[idx][pm.FIELD_NEXT_TOKEN])
        # Validate update of existing log group:
        update_log_group = LogGroup(pos_data[0]['path'], None, pos_data[0]['latest_time'], 30)
        update_log_group.next_token = 'new_next_token'
        pm.update_position_file(update_log_group)
        with open(position_file_path, 'r') as pf:
            test_yaml = yaml.safe_load(pf)
            self.assertIsNotNone(test_yaml)
            self.assertEqual(2, len(test_yaml))
            self.assertEqual(test_yaml[0][pm.FIELD_PATH], update_log_group.path)
            self.assertEqual(test_yaml[0][pm.FIELD_LATEST_TIME], update_log_group.latest_time)
            self.assertEqual(test_yaml[0][pm.FIELD_NEXT_TOKEN], update_log_group.next_token)
        # Validate adding new log group:
        new_log_group = LogGroup('brand-new-path', None, 1681389982, 30)
        new_log_group.next_token = 'this-is-new-too'
        pm.update_position_file(new_log_group)
        with open(position_file_path, 'r') as pf:
            test_yaml = yaml.safe_load(pf)
            self.assertIsNotNone(test_yaml)
            self.assertEqual(3, len(test_yaml))
            self.assertEqual(test_yaml[2][pm.FIELD_PATH], new_log_group.path)
            self.assertEqual(test_yaml[2][pm.FIELD_LATEST_TIME], new_log_group.latest_time)
            self.assertEqual(test_yaml[2][pm.FIELD_NEXT_TOKEN], new_log_group.next_token)

    def test_sync_position_file(self):
        position_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), self.NEW_POS_FILE)
        pm = PositionManager(position_file_path)
        log_groups = [LogGroup('first/log/group', None, 1681389974, 30),
                      LogGroup('second/log/group', None, 1681389974, 30)]
        pos_data = [{'latest_time': 1681389974, 'next_token': 'some-token-123', 'path': 'first/log/group'},
                    {'latest_time': 1681389974, 'next_token': 'another-token-456', 'path': 'second/log/group'},
                    {'latest_time': 1681389974, 'next_token': 'third-token-789', 'path': 'third/log/group'}]
        with open(position_file_path, 'w') as pf:
            yaml.dump(pos_data, pf)
        self.assertTrue(position_file_path)
        with open(position_file_path, 'r') as pf:
            test_yaml = yaml.safe_load(pf)
            self.assertIsNotNone(test_yaml)
            self.assertEqual(3, len(test_yaml))
            for idx, pd in enumerate(test_yaml):
                self.assertEqual(pos_data[idx][pm.FIELD_PATH], test_yaml[idx][pm.FIELD_PATH])
                self.assertEqual(pos_data[idx][pm.FIELD_LATEST_TIME], test_yaml[idx][pm.FIELD_LATEST_TIME])
                self.assertEqual(pos_data[idx][pm.FIELD_NEXT_TOKEN], test_yaml[idx][pm.FIELD_NEXT_TOKEN])
        pm.sync_position_file(log_groups)
        with open(position_file_path, 'r') as pf:
            test_yaml = yaml.safe_load(pf)
            self.assertIsNotNone(test_yaml)
            self.assertEqual(2, len(test_yaml))
            for idx, pd in enumerate(test_yaml):
                self.assertEqual(pos_data[idx][pm.FIELD_PATH], test_yaml[idx][pm.FIELD_PATH])
                self.assertEqual(pos_data[idx][pm.FIELD_LATEST_TIME], test_yaml[idx][pm.FIELD_LATEST_TIME])
                self.assertEqual(pos_data[idx][pm.FIELD_NEXT_TOKEN], test_yaml[idx][pm.FIELD_NEXT_TOKEN])


if __name__ == '__main__':
    unittest.main()
