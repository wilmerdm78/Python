#!/usr/bin/env python

import csv
import io
import avro.io
import avro.schema

from confluent_kafka import Producer

if __name__ == "__main__":

	#Configure the Producer
	producer_config = {
    	# User-specific properties that you must set
    	'bootstrap.servers': <BOOTSTRAP_SERVER>,
    	'sasl.username': <USERNAME>,
    	'sasl.password': <PASSWORD>,
    	# Fixed properties
    	'security.protocol': 'SASL_SSL',
    	'sasl.mechanisms':   'PLAIN',
    	'acks':              'all'
	}

	#Create Kafka producer
	producer = Producer(producer_config)

	#Idenfiy topic and file path
	topic_name = <KAFKA_TOPIC>
	file_path = '/home/ubuntu/data/vodclickstream_uk_movies_03.csv'

	#Define the Avro schema
	avro_schema_str = """
	{
		"type": "record",
		"name": "Netflix_stream",
		"fields": [
			{"name": "record_id", "type": "int"},
			{"name": "datetime", "type": "string", "logicalType": "timestamp-millis"},
			{"name": "duration", "type": "int"},
			{"name": "title", "type": "string"},
			{"name": "genres","type": "string"},
			{"name": "release_date", "type": "string", "logicalType": "timestamp-millis"},
			{"name": "movie_id", "type": "string"},
			{"name": "user_id", "type": "string"}
		]
	}
	"""

	avro_schema = avro.schema.parse(avro_schema_str)

	with open(file_path, 'r') as csv_file:
		csv_reader = csv.DictReader(csv_file)
		for row in csv_reader:
			writer = avro.io.DatumWriter(avro_schema)
			bytes_writer = io.BytesIO()
			encoder = avro.io.BinaryEncoder(bytes_writer)
			writer.write({
				"record_id": int(row["record_id"]),
				"datetime": (row["datetime"]),
				"duration": int(row["duration"]),
				"title": (row["title"]),
				"genres": (row["genres"]),
				"release_date": (row["release_date"]),
				"movie_id": (row["movie_id"]),
				"user_id": (row["user_id"])}, encoder)
			writer_bytes = bytes_writer.getvalue()
			producer.produce(topic_name, writer_bytes)
	producer.flush()

