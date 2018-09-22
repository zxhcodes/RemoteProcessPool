from concurrent.futures import ProcessPoolExecutor
from kombu.mixins import ConsumerMixin
from kombu.log import get_logger
from functools import partial
from kombu import Queue
from settings import cached_queue, broker_url

logger = get_logger(__name__)

def add(nums):
    return sum(nums)

class Worker(ConsumerMixin):

    def __init__(self, connection):
        self.connection = connection
        self.task_queue = Queue(cached_queue)
        self.pool = ProcessPoolExecutor(max_workers=8)
        self.handler = None

    def get_consumers(self, Consumer, default_channel):
        return [Consumer(queues=[self.task_queue], callbacks=[self.receive_msg], no_ack=True)]

    def register_hander(self, func):
        if not callable(func):
            logger.error("%s is not a function", str(func))
            return False
        self.handler = func
        return True

    def receive_msg(self, body, msg):
        reply_to = msg.properties.get('reply_to', None)
        correlation_id = msg.properties.get('correlation_id', None)
        if not (reply_to and correlation_id):
            return

        logger.info("recieved: %s %s", correlation_id, body)

        future = self.pool.submit(self.handler, body)

        future.add_done_callback(partial(self.response, correlation_id, reply_to))

    def response(self, correlation_id, reply_to, future):
        body = future.result()
        with self.connection.Producer(serializer='json') as producer:
            producer.publish(
                body,
                correlation_id=correlation_id,
                routing_key=reply_to,
                retry=True,
            )
        logger.info("succeeded: %s %s", correlation_id, body)

    def run(self, _tokens=1, **kwargs):
        if not self.handler:
            logger.error("can't find any handler for msg")
            return
        super().run(_tokens, **kwargs)


if __name__ == '__main__':
    from kombu import Connection
    from kombu.utils.debug import setup_logging

    setup_logging(loglevel='INFO', loggers=[''])

    with Connection(broker_url) as conn:
        try:
            worker = Worker(conn)
            worker.register_hander(add)
            worker.run()
        except KeyboardInterrupt:
            print('bye bye')