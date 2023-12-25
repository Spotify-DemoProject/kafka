
def create_topic(broker:str, name:str, partitions:int=1, replications:int=1):
    from confluent_kafka.admin import AdminClient, NewTopic

    conf = {'bootstrap.servers': broker}
    client = AdminClient(conf)

    topic = NewTopic(name, 
                     num_partitions=partitions,
                     replication_factor=replications)
    client.create_topics([topic])
