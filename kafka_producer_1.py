from kafka import KafkaProducer
if __name__ == '__main__':
    try:
        producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092')
        while True:
            print(b'this text5')
            producer.send(topic='ravi3',  value=b'this text5')

    except Exception as e:
        print(e)
