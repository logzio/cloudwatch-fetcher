import logging
import unittest
import os

from src.manager import Manager


class ManagerTests(unittest.TestCase):
    def test_no_logzio_token(self):
        manager = Manager()
        os.environ[manager.ENV_LOGZIO_TOKEN] = ''
        manager.run()
        self.assertLogs('src.manager', logging.ERROR)

    def test_no_aws_credentials(self):
        manager = Manager()
        os.environ[manager.ENV_LOGZIO_TOKEN] = 'some-token'
        os.environ['AWS_ACCESS_KEY_ID'] = ''
        os.environ['AWS_SECRET_ACCESS_KEY'] = ''
        os.environ['AWS_SESSION_TOKEN'] = ''
        os.environ['AWS_PROFILE'] = ''
        os.environ['AWS_CONFIG_FILE'] = ''
        manager.run()
        self.assertLogs('src.manager', logging.ERROR)


if __name__ == '__main__':
    unittest.main()
