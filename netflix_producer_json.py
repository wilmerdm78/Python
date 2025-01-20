#!/usr/bin/env python

import  pandas as pd
import csv
from confluent_kafka import Producer

#set variables
topic = <KAFKA_TOPIC>
file_path = '/home/ubuntu/data/vodclickstream_uk_movies_03.csv'

if __name__ == '__main__':

    producer_config = {
        # User-specific properties that you must set
        'bootstrap.servers': <BOOTSTRAP_SERVER>,
        'sasl.username':     <USERNAME>,
        'sasl.password':     <PASSWORD>,
        # Fixed properties
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms':   'PLAIN',
        'acks':              'all'
    }

    # Create Producer instance
    producer = Producer(producer_config)

    # Read source csv file into a dataframe.
    df = pd.read_csv(file_path)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    # Function to serialize data to JSON
    def serialize_to_json(df_row):
        return df_row.to_json().encode('utf-8')

    # Send data to Kafka
    for index, row in df.iterrows():
        producer.produce(topic, key=str(row['record_id']), value=serialize_to_json(row), callback=delivery_callback)

    # Block until the messages are sent.
    producer.poll(0)
    producer.flush()
