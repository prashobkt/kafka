from kafka import KafkaConsumer

consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest'
                         )
consumer.subscribe(topics='ravi5')

for mp1 in consumer:
    print(mp1)
    print(mp1.value)
    print("Partition Number : " + str(mp1.partition))
    print("offset number    : " + str(mp1.offset))
    print("timestamp        : "+str("timestamp"))
