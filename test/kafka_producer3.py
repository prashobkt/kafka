import json
import time
from datetime import datetime

from bson import json_util
from kafka import KafkaProducer


if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092', acks='all',
                             client_id="tester"
                             )
    # JSON
    # key: value
    #
    data = {"source": "ravim1",
            "name": "BL2_SET_V",
            "value": 85.0, "mf": 1,
            "date": datetime.now().strftime('%d/%m/%y-%H:%M:%S')
            }

    producer.send(topic='ravi1', key=None,
                  value=json.dumps(data, default=json_util.default).encode('utf-8'))

    #producer.send(topic='ravi1', value=b'this text5')
    producer.flush()
    producer.close()

