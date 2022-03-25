import json
import time
from datetime import datetime

from bson import json_util, BSON
from kafka import KafkaProducer


def on_send_error(error):
    print(error)


if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092', acks='all',
                             client_id="tester",
                             retries=2,
                             retry_backoff_ms=120 * 1000,
                             request_timeout_ms=120 * 1000

                             )
    # JSON
    # key: value
    #
    data = {"source": "Producer_5",
            "name": "test",
            "value": 90,
            "mf": 1,
            "date": datetime.now().strftime('%d/%m/%y-%H:%M:%S')
            }
    partition_v=1
    producer_sent = producer.\
        send(topic='ravi1',
             key=None,
             partition= partition_v,
             value=json.dumps(data, default=json_util.default).encode('utf-8')) \
        .add_errback(on_send_error)
    producer.flush()
    producer.close()
