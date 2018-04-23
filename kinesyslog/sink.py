import logging
import math
import time
from asyncio import get_event_loop
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from gzip import compress

from boto3 import Session

import ujson as json

from . import constant

logger = logging.getLogger(__name__)


class MessageSink(object):
    __slots__ = ['spool', 'loop', 'executor', 'size', 'count', 'messages', 'flushed', 'message_class', 'account', 'raw']

    def __init__(self, spool, message_class, raw):
        self.spool = spool
        self.message_class = message_class
        self.raw = raw
        self.loop = get_event_loop()
        self.executor = ProcessPoolExecutor()
        self.executor._start_queue_management_thread()
        self._schedule_flush()
        self.clear()
        self.account = '000000000000'
        try:
            session = Session(profile_name=spool.profile_name)
            client = session.client('sts', config=spool.config)
            self.account = client.get_caller_identity()['Account']
        except Exception:
            logger.warn('Unable to determine AWS Account ID; using default value.')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.flush()

    async def write(self, source, message, timestamp):
        self.messages[source].append((message, timestamp))
        self.size += len(message)
        self.count += 1
        if self.size > constant.FLUSH_SIZE:
            await self.flush_async()
        return len(message)

    def clear(self):
        self.size = 0
        self.count = 0
        self.messages = defaultdict(list)
        self.flushed = time.time()

    async def flush_async(self):
        self.loop.run_in_executor(self.executor, self._spool_messages, self.spool, self.messages, self.size, self.message_class, self.account, self.raw)
        self.clear()

    def flush(self):
        self._spool_messages(self.spool, self.messages, self.size, self.message_class, self.account, self.raw)
        self.clear()

    def _schedule_flush(self):
        self.loop.call_later(constant.TIMER_INTERVAL, self._flush_timer)

    def _flush_timer(self):
        logger.debug('flush timer: messages={0} size={1} age={2}'.format(self.count, self.size, time.time() - self.flushed))
        if self.messages and time.time() - self.flushed >= constant.FLUSH_TIME:
            self.loop.create_task(self.flush_async())
        self._schedule_flush()

    @classmethod
    def _spool_messages(cls, spool, messages, size, message_class, account, raw):
        for i_source, i_messages in messages.items():
            events = list(message_class.create_events(i_source, i_messages))
            record = cls._prepare_record(i_source, events, message_class.name, account)
            compressed_record = cls._compress_record(raw, record)
            logger.debug('Events for {0} compressed from {1} to {2} bytes (with JSON framing)'.format(i_source, size, len(compressed_record)))

            if len(compressed_record) > constant.MAX_RECORD_SIZE:
                data = record['logEvents']

                while data:
                    cursor = 1
                    old_compressed_record = None

                    while True:
                        record_part = cls._prepare_record(i_source, data[0:cursor], message_class.name, account)
                        new_compressed_record = cls._compress_record(raw, record_part)

                        if len(new_compressed_record) > constant.MAX_RECORD_SIZE or len(data) == cursor:
                            if old_compressed_record:
                                spool.write(old_compressed_record)
                            else:
                                logger.warning("Record {0} cannot be written".format(record_part))
                            data = data[cursor:]
                            break
                        else:
                            cursor += 1
                            old_compressed_record = new_compressed_record
            else:
                spool.write(compressed_record)

    @classmethod
    def _prepare_record(cls, source, events, class_name, account):
        return {
            'owner': account,
            'logGroup': class_name,
            'logStream': source,
            'subscriptionFilters': [class_name],
            'messageType': 'DATA_MESSAGE',
            'logEvents': events,
        }

    @classmethod
    def _compress_record(cls, raw, record):
        if raw:
            events = record['logEvents']
            messages = [m['message'].split(' ', 3)[3] + '\n' for m in events] # Leave only message
            return ''.join(messages).encode()
        return compress(MessageSink.serialize(record))

    @classmethod
    def serialize(cls, data):
        return json.dumps(data).encode()
