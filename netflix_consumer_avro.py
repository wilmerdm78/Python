#!/usr/bin/env python

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer


# Configuration
bootstrap_servers = <BOOTSTRAP_SERVER>
username = <USER_NAME>
password = <PASSWORD>
schema_registry_url = <REGISTRY_URL>
topic_name = <KAFKA_TOPIC>
group_id = <CLUSTER_GROUP_ID>

#Configure Kafka and Schema Registry
schema_registry_conf = {
        'url': <REGISTRY_URL>,
        'basic.auth.user.info': f'<REGISTRY_USERNAME>:<REGISTRY_PASSWORD>'
        }

#Configure the Consumer
consumer_config = {
    # User-specific properties that you must set
    'bootstrap.servers': bootstrap_servers,
    'sasl.username': username,
    'sasl.password': password,
    'group.id': group_id,
    'auto.offset.reset': 'earliest',
    # Fixed properties
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms':   'PLAIN'
    }

#Schema Registry client
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

#Avro Deserializer
value_deserializer = AvroDeserializer(schema_registry_client)

#Create Consumer
consumer = Consumer(consumer_config)
consumer.subscribe([topic_name])

# Consume messages
while True:
    try:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print('Reached end of partition')
            else:
                print('Error while consuming message: {}'.format(msg.error()))
        else:
            value = msg.value()
            print("Received message: {}".format(value))

    except KeyboardInterrupt:
        break

# Close consumer
consumer.close()
