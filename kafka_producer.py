import datetime
import json
import time
import traceback

import pytz as pytz
from bson import json_util

from kafka import KafkaProducer


def on_send_success(record_metadata):
    print("==============================================")
    print("RECORD TOPIC > " + record_metadata.topic)
    print("RECORD PARTITION > " +  str(record_metadata.partition))
    print("RECORD OFFSET > " + str(record_metadata.offset))
    print("RECORD TIME > " + str(record_metadata.timestamp))
    print("RECORD TIME > " + str(record_metadata.timestamp))

    print("================================================")


def on_send_error(excp):
    print('error ', exc_info=excp)
    # handle exception

if __name__ == '__main__':

    try:

        producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092', acks='all')

        while True:
            ltz = pytz.timezone("Asia/Kolkata")
            now = datetime.datetime.now()
            time_stamp = now.strftime('%d/%m/%y-%H:%M:%S')


            # data = {"source_key": "3BjWxsZbBYQ9lSn", "stream_name": "BL2_SET_V", "value": 85.0, "mf": 1,
            #         "date": "18/01/21 16:17:10"}
            data = {"source_key": "ravim1", "stream_name": "BL2_SET_V", "value": 85.0, "mf": 1,
                    "date":time_stamp }

            producer.send(topic='ravi1', key=time_stamp.encode('utf-8'),
                          value=json.dumps(data, default=json_util.default).encode('utf-8'))
                # \
                # .add_callback(on_send_success).add_errback(on_send_error)

            time.sleep(2)

        # producer.flush()
        # producer.close()

    except Exception as e:
        print(e.args)
    except:
        print(traceback.format_exc())
