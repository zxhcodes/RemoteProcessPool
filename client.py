from kombu import Queue, Producer, Consumer
from kombu import Connection
import uuid
from settings import cached_queue, broker_url

class RemoteProcessClient(object):

    def __init__(self, connection):
        self.connection = connection
        self.task_queue = Queue(cached_queue)
        self.result_queue = Queue(str(uuid.uuid4()), exclusive=True, auto_delete=True)

    def on_response(self, msg):
        correlation_id = msg.properties['correlation_id']
        if correlation_id == self.correlation_id:
            self.response = msg.payload

    def call(self, x, y):
        self.response = None
        self.correlation_id = str(uuid.uuid4())
        with Producer(self.connection, serializer='json') as producer:
            producer.publish(
                [x, y],
                routing_key=cached_queue,
                declare=[self.result_queue, self.task_queue],
                reply_to=self.result_queue.name,
                correlation_id=self.correlation_id
            )
        with Consumer(self.connection, on_message=self.on_response, queues=[self.result_queue], no_ack=True):
            while self.response is None:
                self.connection.drain_events()

        return self.response


if __name__ == "__main__":
    with Connection(broker_url) as conn:
        rpc = RemoteProcessClient(conn)
        rst = rpc.call(1, 1)
        print(rst)