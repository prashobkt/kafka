from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition

consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'],
                         group_id='test',
                         auto_offset_reset='earliest',
                         enable_auto_commit=False)

partition = TopicPartition('ravi1', 1)
partition2 = TopicPartition('ravi1', 2)
consumer.assign([partition,partition2])
# consumer.subscribe(topics='ravi1')
offset_array=[]
for mp1 in consumer:
    # Received message'
    message = mp1.value
    print(message)


    
    #=============================
    #Commit
    #==========================
    meta =consumer.partitions_for_topic(mp1.topic)
    tp = TopicPartition(mp1.topic, mp1.partition)
    options={}
    options[tp] = OffsetAndMetadata(mp1.offset , meta)
    consumer.commit(options)





