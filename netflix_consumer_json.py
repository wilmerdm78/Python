#!/usr/bin/env python

import json
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer

# Configuration
bootstrap_servers = 'pkc-p11xm.us-east-1.aws.confluent.cloud:9092'
username = 'TYFI3BPHPFXI33JG'
password = 'fRs3XPmDl0f/QJTtO0+3o4GOhZQAOMpvjrXNO8y27s/ru8fusdtPl6Wp7zUfyGXB'
schema_registry_url = 'https://psrc-6kq702.us-east-1.aws.confluent.cloud'
topic_name = 'Netflix_json'
group_id = 'lkc-2kpv6y'
json_schema_file = 'json_schema.json'

#Configure Kafka and Schema Registry
schema_registry_conf = {
        'url': 'https://psrc-6kq702.us-east-1.aws.confluent.cloud',
        'basic.auth.user.info': f'BLY3JSFSCTDBORGC:gypKJdK7TSspbGpCQe4MYkzjvllUEFvf8aQOdwEezE3PSnmMVW80rEEt3ghiRLGx'
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

#Open JSON schema file and load to Python dictionary
with open(json_schema_file, "r") as file:
	json_schema = json.load(file)

#Schema Registry client
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

#Avro Deserializer
value_deserializer = JSONDeserializer(schema_registry_client, json_schema)

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