import unittest
import datetime

from src.log_group import LogGroup


class LogGroupTests(unittest.TestCase):
    def test_log_group_namespace_exists(self):
        log_group = LogGroup('/aws/rds/cluster/my-log-group', {'key1': 'val2'}, 1681389974, 20)
        self.assertEqual('aws/rds', log_group.namespace)

    def test_log_group_namespace_not_exists(self):
        log_group = LogGroup('unknown-log-group', {'key1': 'val2'}, 1681389974, 20)
        self.assertEqual('', log_group.namespace)

    def test_latest_timestamp(self):
        latest = 1681389974
        interval = 20
        log_group = LogGroup('/aws/rds/cluster/my-log-group', {'key1': 'val2'}, latest, interval)
        dt = datetime.datetime.fromtimestamp(latest)
        minutes_ago = dt - datetime.timedelta(minutes=interval)
        unix_seconds = int(minutes_ago.timestamp())
        self.assertEqual(unix_seconds, log_group.latest_time)


if __name__ == '__main__':
    unittest.main()
