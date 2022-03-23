from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition


if __name__ == '__main__':
    consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'],
                         group_id='test',
                         auto_offset_reset='earliest',
                         enable_auto_commit=False,

                             )
    consumer.subscribe(topics='ravi1')
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





