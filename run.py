from flask import Flask
from redis import Redis
from kafka import SimpleProducer, KafkaClient, KafkaConsumer

import json

from threading import Thread

app = Flask(__name__)
redis = Redis(host='redis', port=6379)

kafka = KafkaClient('kafka:9092')

topic = 'one_tets'

# consumer = KafkaConsumer(topic, group_id='download', bootstrap_servers=['kafka:9092'],
#                         auto_offset_reset = 'earliest',
#                         value_deserializer = lambda m: json.loads(m.decode()))
producer = SimpleProducer(kafka)


@app.route('/')
def hello():
    count = redis.incr('hits')
    return 'Hello World! I have been seen {} times.\n'.format(count)

@app.route('/send')
def sendTopic():
    
    data = {
        'name': 'test',
        'message': 'recieved'
    }
    message = json.dumps(data, indent=2)
    producer.send_messages(topic, message.encode())

    return 'send message kafka'

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)

    