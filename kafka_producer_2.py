from kafka import KafkaProducer


def on_send_success(record_metadata):
    # print("==============================================")
    # print("RECORD TOPIC > " + record_metadata.topic)
    print("RECORD PARTITION > " +str(record_metadata.partition))
    print("RECORD OFFSET > " + str(record_metadata.offset))
    print("RECORD TIME > " + str(record_metadata.timestamp))
    # print("RECORD TIME > " + str(record_metadata.timestamp))

    print("================================================")


if __name__ == '__main__':
    try:
        producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092')
        while True:
            print(b'this text5')
            producer.send(topic='ravi5',  value=b'this text5').add_callback(on_send_success)

    except Exception as e:
        print(e)
