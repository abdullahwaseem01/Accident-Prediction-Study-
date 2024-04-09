from confluent_kafka import Consumer, KafkaError
import pandas as pd
from google.cloud import bigquery
import io
import json 
from google.cloud.exceptions import NotFound
import numpy as np

consumer = Consumer({
    'bootstrap.servers': '',
    'group.id': '',
    'auto.offset.reset': 'earliest',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': '',
    'sasl.password': '',
})
consumer.subscribe(['accident_risk'])

client = bigquery.Client.from_service_account_json('path/to/your/credentials.json')
table_id = 'modular-tube-419117.inD.accident_risk'


schema = [
    bigquery.SchemaField("locationId", "INTEGER"),
    bigquery.SchemaField("numVehicles", "FLOAT"),
    bigquery.SchemaField("estimatedRisk", "FLOAT"),
    bigquery.SchemaField("weekday", "STRING"),
    bigquery.SchemaField("startTime", "INTEGER"),
    bigquery.SchemaField("duration", "FLOAT"),
    bigquery.SchemaField("avgSpeed", "FLOAT"),
    bigquery.SchemaField("latLocation", "FLOAT"),
    bigquery.SchemaField("lonLocation", "FLOAT"),
]

table = bigquery.Table(table_id, schema=schema)

try:
    client.get_table(table_id)
    print("Table {} already exists.".format(table_id))
except NotFound:
    print("Table {} is not found. Creating now...".format(table_id))
    table = client.create_table(table) 
    print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

def consume_messages(consumer, table_id, client):
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('%% %s [%d] reached end at offset %d\n' %
                          (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaError(msg.error())
            else:
                try:
                    message_string = msg.value().decode('utf-8')
                    message_dict = json.loads(message_string)
                    for key in message_dict.keys():
                        if isinstance(message_dict[key], float) and np.isnan(message_dict[key]):
                            message_dict[key] = None
                    row_to_insert = [message_dict]

                    errors = client.insert_rows_json(table_id, row_to_insert)
                    if errors == []:
                        print(f"Successfully inserted row into BigQuery: {row_to_insert}")
                    else:
                        print(f"Errors occurred while inserting rows: {errors}")
                except json.JSONDecodeError as e:
                    print(f"JSON decode error: {e}")
                except Exception as e:
                    print(f"Unexpected error: {e}")
    except KafkaError as e:
        print(f"Kafka error: {e}")
    finally:
        consumer.close()


if __name__ == "__main__":
    consume_messages(consumer, table_id, client)

