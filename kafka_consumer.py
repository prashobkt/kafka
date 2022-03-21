import json

from kafka import KafkaConsumer, TopicPartition
# from pymongo import MongoClient
# from json import loads
# from kafka import KafkaClient

# p = TopicPartition('xyx', '0')
# consumer.assign([p])
# consumer.seek(p, 43809904)
# auto_offset_reset='earliest',
consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'],
                         # api_version=(0, 10, 1),
                         auto_offset_reset='earliest',
                         enable_auto_commit=False,
                         group_id='my-group',
                         # value_deserializer=lambda x: loads(x.decode('utf-8')),
                         # consumer_timeout_ms=60*1000
                         )
#partition = TopicPartition('REALTIME', 0)
#consumer.assign([partition])
#consumer.seek(partition, 28)
consumer.subscribe(topics='ravi1')

for message in consumer:
    # message = message.value
    print("RECORD TOPIC" + message.topic)
   # print("RECORD VALUE" + message.value)
    # print(message.)
    print("RECORD PARTITION > " + str(message.partition))
    print("RECORD OFFSET > " + str(message.offset))
    print("RECORD MESSAGE KEY > " + str(message.key))
    m = message.value.decode('utf-8')
    print("RECORD MESSAGE VALUE > " + m)
    #print( message.)
    #consumer.commit()
