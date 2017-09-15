from kafka import KafkaClient, KafkaConsumer

import json
import time

topic = 'one_hjh'


for i in range(20):
    try:
        consumer = KafkaConsumer(topic, group_id='consumer', bootstrap_servers=['kafka:9092'],
                                enable_auto_commit=True,
                                value_deserializer = lambda m: json.loads(m.decode()))

        for msg in consumer:
            print (msg.value)
    except Exception as e:
        print (e)
        time.sleep(5)