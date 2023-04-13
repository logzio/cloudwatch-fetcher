import logging
import os
import unittest
from src.config_reader import ConfigReader


class ConfigReaderTests(unittest.TestCase):
    CONFIG_FILE = 'fixture/config.yaml'
    CONFIG_INVALID_FILE = 'fixture/invalid_config.yaml'
    CONFIG_INVALID_INTERVAL_FILE = 'fixture/invalid_interval.yaml'
    CONFIG_NO_AWS_REGION_FILE = 'fixture/no_aws_region.yaml'
    LATEST_TIME = 1681393953
    INTERVAL = 10

    def setUp(self):
        config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), self.CONFIG_FILE)
        self.config_reader = ConfigReader(config_file)

    def set_alternative_config_reader(self, file_path):
        config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), file_path)
        self.config_reader = ConfigReader(config_file)

    def test_get_log_groups(self):
        log_groups = self.config_reader.get_log_groups(self.LATEST_TIME, self.INTERVAL)
        self.assertIsNotNone(log_groups)
        self.assertEqual(3, len(log_groups))
        for lg in log_groups:
            if lg.path == '/aws/lambda/my-lambda':
                self.assertEqual(2, len(lg.custom_fields))
            elif lg.path == '/other/log/group':
                self.assertEqual(1, len(lg.custom_fields))
            elif lg.path == 'thisisaloggroup':
                self.assertIsNone(lg.custom_fields)
            else:
                self.assertEqual(True, False, msg=f'Invalid log group {lg.path}!')

    def test_get_log_groups_invalid_config(self):
        self.set_alternative_config_reader(self.CONFIG_INVALID_FILE)
        log_groups = self.config_reader.get_log_groups(self.LATEST_TIME, self.INTERVAL)
        self.assertIsNone(log_groups)
        self.assertLogs('src.config_reader', level=logging.ERROR)

    def test_get_time_interval(self):
        time_interval = self.config_reader.get_time_interval()
        self.assertEqual(10, time_interval)

    def test_get_time_interval_invalid(self):
        self.set_alternative_config_reader(self.CONFIG_INVALID_INTERVAL_FILE)
        time_interval = self.config_reader.get_time_interval()
        self.assertEqual(0, time_interval)
        self.assertLogs('src.config_reader', level=logging.WARNING)

    def test_get_aws_region(self):
        aws_region = self.config_reader.get_aws_region()
        self.assertEqual('us-east-1', aws_region)

    def test_get_aws_region_no_region(self):
        self.set_alternative_config_reader(self.CONFIG_NO_AWS_REGION_FILE)
        aws_region = self.config_reader.get_aws_region()
        self.assertEqual('', aws_region)
        self.assertLogs('src.config_reader', level=logging.DEBUG)


if __name__ == '__main__':
    unittest.main()
