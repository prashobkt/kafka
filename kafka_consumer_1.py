from kafka import KafkaConsumer

consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'])
consumer.subscribe(topics='ravi3')

for mp1 in consumer:
    print(mp1.value)



