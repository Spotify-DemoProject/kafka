
def consume_messages(group_id:str, topic:str, key:str, partition:int=0):
    from confluent_kafka import Consumer, KafkaError
    from configparser import ConfigParser
    import os, sys

    config_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../config/config.ini")
    config = ConfigParser()
    config.read(config_dir)
    broker = config.get("KAFKA", "broker")
    
    conf = {
        'bootstrap.servers': broker,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            if msg.partition() == partition and msg.key() == key:
                print('Received message: {}'.format(msg.value().decode('utf-8')))

    except KeyboardInterrupt:
        pass
    
    finally:
        consumer.close()


if __name__ == '__main__':
    pass