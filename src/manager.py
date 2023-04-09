import json
import logging
import os
import threading
import signal
import boto3
import time

from .config_reader import ConfigReader
from .log_group import LogGroup
from .logzio_shipper import LogzioShipper

logger = logging.getLogger(__name__)


class Manager:
    SHIPPER = 'cw-fetcher'
    DEFAULT_TYPE = 'cloudwatch'
    CONFIG_FILE = 'shared/config.yaml'
    _DEFAULT_INTERVAL = 5
    _DEFAULT_LOGZIO_LISTENER = 'https://listener.logz.io:8071'
    ENV_LOGZIO_TOKEN = 'LOGZIO_LOG_SHIPPING_TOKEN'
    ENV_LOGZIO_LISTENER = 'LOGZIO_LISTENER'
    KEY_NEXT_TOKEN = 'nextToken'
    KEY_EVENTS = 'events'
    KEY_MESSAGE = 'message'
    KEY_TIMESTAMP = 'timestamp'
    FIELD_NAMESPACE = 'namespace'
    FIELD_LOG_GROUP = 'logGroup'
    FIELD_LOG_STREAM = 'logStream'
    FIELD_OWNER = 'owner'
    FIELD_SHIPPER = 'shipper'
    FIELD_TYPE = 'type'
    FIELD_ID = 'id'
    FIELD_LOG_LEVEL = 'log_level'
    FIELD_TIMESTAMP = '@timestamp'
    LOG_LEVELS = ['ALERT', 'TRACE', 'DEBUG', 'NOTICE', 'INFO', 'WARN',
                  'WARNING', 'ERROR', 'ERR', 'CRITICAL', 'CRIT',
                  'FATAL', 'SEVERE', 'EMERG', 'EMERGENCY']

    def __init__(self):
        self._threads = []
        self._event = threading.Event()
        self._lock = threading.Lock()
        self._log_groups = []
        self._interval = self._DEFAULT_INTERVAL  # minutes
        self._logzio_token = ''
        self._logzio_listener = ''
        self._aws_region = ''
        self.start_time = int(time.time())
        self._account_id = ''

    def run(self):
        logger.info('Starting Cloudwatch Fetcher')
        if not self._get_logzio_credentials():
            return
        if not self._read_data_from_config():
            return
        self._account_id = self._get_account_id()
        for log_group in self._log_groups:
            self._threads.append(threading.Thread(target=self._run_scheduled_log_collection, args=(log_group,)))
        for thread in self._threads:
            thread.start()

        signal.sigwait([signal.SIGINT, signal.SIGTERM])
        self.__exit_gracefully()

    def _get_account_id(self):
        try:
            session = boto3.session.Session(region_name=self._aws_region)
            sts_client = session.client('sts')
        except Exception as e:
            logger.error(f'Encountered error while creating sts client: {e}')
            return ''
        try:
            account_id = sts_client.get_caller_identity()['Account']
            logger.debug(f'AWS account id: {account_id}')
            return account_id
        except Exception as e:
            logger.error(f'Encountered error while getting AWS account id: {e}')
            return ''

    def _read_data_from_config(self):
        config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), self.CONFIG_FILE)
        logger.info(f'Config file path: {config_file}')
        config_reader = ConfigReader(config_file)
        if config_reader is None:
            return False
        self._log_groups = config_reader.get_log_groups(self.start_time, self._DEFAULT_INTERVAL)
        if self._log_groups is None or len(self._log_groups) == 0:
            return False
        region = config_reader.get_aws_region()
        if region != '':
            self._aws_region = region
            logger.info(f'AWS region: {self._aws_region}')
        else:
            logger.error(f'Field {config_reader.KEY_LOG_GROUP_REGION} not specified')
            return False
        interval = config_reader.get_time_interval()
        if interval != 0:
            self._interval = interval
            logger.debug(f'Set {config_reader.KEY_INTERVAL} to: {self._interval}')
        else:
            logger.info(f'Reverting {config_reader.KEY_INTERVAL} to default value: {self._DEFAULT_INTERVAL}')
        return True

    def _get_logzio_credentials(self):
        self._logzio_token = os.getenv(self.ENV_LOGZIO_TOKEN)
        if self._logzio_token is None:
            logger.error(f'Env var {self.ENV_LOGZIO_TOKEN} must be set!')
            return False
        self._logzio_listener = os.getenv(self.ENV_LOGZIO_LISTENER, self._DEFAULT_LOGZIO_LISTENER)
        return True

    def _run_scheduled_log_collection(self, log_group):
        logzio_shipper = LogzioShipper(self._logzio_listener, self._logzio_token)

        while True:
            thread = threading.Thread(target=self._fetch_and_send, args=(log_group, logzio_shipper,))

            thread.start()
            thread.join()

            timeout_seconds = self._interval * 60
            if self._event.wait(timeout=timeout_seconds):
                logger.info('Terminating...')
                break

    def _fetch_and_send(self, log_group, logzio_shipper):
        now = int(time.time())
        resp = None
        new_logs = False
        try:
            session = boto3.session.Session(region_name=self._aws_region)
            cw_client = session.client('logs')
        except Exception as e:
            logger.error(f'Encountered error while creating Cloudwatch client: {e}')
            return

        additional_fields = self._get_additional_fields(log_group)

        while True:
            try:
                logger.debug(f'Start time: {log_group.latest_time}')
                logger.debug(f'End time: {now}')
                logger.debug(f'Next token: {log_group.next_token}')
                if log_group.next_token != '':
                    resp = cw_client.filter_log_events(logGroupName=log_group.path,
                                                       startTime=log_group.latest_time * 1000,
                                                       endTime=now * 1000,
                                                       nextToken=log_group.next_token)
                else:
                    resp = cw_client.filter_log_events(logGroupName=log_group.path,
                                                       startTime=log_group.latest_time * 1000,
                                                       endTime=now * 1000)
                if self.KEY_NEXT_TOKEN in resp:
                    log_group.next_token = resp[self.KEY_NEXT_TOKEN]
                if len(resp[self.KEY_EVENTS]) == 0:
                    logger.info('No new logs at the moment')
                    break
                new_logs = True
                logger.info(f'Got {len(resp[self.KEY_EVENTS])} new logs')
                self._process_events(resp[self.KEY_EVENTS], additional_fields, logzio_shipper)
                # if next_token == '':
                #     break
                break
            except Exception as e:
                logger.error(f'Error while trying to get log events for {log_group.path}: {e}')
                break

        log_group.latest_time = now
        if new_logs:
            logzio_shipper.send_to_logzio()

    def _get_additional_fields(self, log_group):
        additional_fields = {self.FIELD_LOG_GROUP: log_group.path,
                             self.FIELD_SHIPPER: self.SHIPPER,
                             self.FIELD_TYPE: self.DEFAULT_TYPE}
        if self._account_id != '':
            additional_fields[self.FIELD_OWNER] = self._account_id
        if log_group.custom_fields is not None and len(log_group.custom_fields) > 0:
            additional_fields.update(log_group.custom_fields)
        if log_group.namespace != '':
            additional_fields[self.FIELD_NAMESPACE] = log_group.namespace
        return additional_fields

    def _process_events(self, events, additional_fields, logzio_shipper):
        for event in events:
            try:
                # add additional fields
                if additional_fields is not None and len(additional_fields) > 0:
                    event.update(additional_fields)
            except Exception as e:
                logger.warning(f'Error while trying to add additional fields: {e}')
            try:
                # rename logStreamName -> logStream (to follow existing conventions for cw logs)
                if 'logStreamName' in event:
                    event[self.FIELD_LOG_STREAM] = event['logStreamName']
                    del event['logStreamName']
            except Exception as e:
                logger.warning(f'Error while trying to rename logStreamName: {e}')
            try:
                # rename eventId -> id (to follow existing conventions for cw logs)
                if 'eventId' in event:
                    event[self.FIELD_ID] = event['eventId']
                    del event['eventId']
            except Exception as e:
                logger.warning(f'Error while trying to rename eventId: {e}')
            try:
                if self.KEY_MESSAGE in event:
                    # remove newline at the end of the message, if exists
                    event[self.KEY_MESSAGE] = event[self.KEY_MESSAGE].rstrip('\n')
                    log_level = self._get_log_level_from_message(str(event[self.KEY_MESSAGE]))
                    if log_level != '':
                        event[self.FIELD_LOG_LEVEL] = log_level
            except Exception as e:
                logger.warning(f'Error while trying to process message: {e}')
            try:
                if self.KEY_TIMESTAMP in event:
                    event[self.FIELD_TIMESTAMP] = event[self.KEY_TIMESTAMP]
                    del event[self.KEY_TIMESTAMP]
            except Exception as e:
                logger.warning(f'Error while trying to process timestamp: {e}')
            log_str = json.dumps(event)
            logzio_shipper.add_log_to_send(log_str)
        # return events

    def _get_log_level_from_message(self, message):
        try:
            start_level = message.index('[')
            end_level = message.index(']')
            log_level = message[start_level + 1:end_level].upper()
            if log_level in self.LOG_LEVELS:
                return log_level
        except ValueError:
            return ''
        return ''

    def __exit_gracefully(self):
        logger.info("Signal caught...")

        self._event.set()

        for thread in self._threads:
            thread.join()
