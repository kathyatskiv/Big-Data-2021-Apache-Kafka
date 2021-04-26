from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
     bootstrap_servers=['<brocker-1-id>:9092', '<brocker-2-id>:9092', '<brocker-3-id>:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group-3')

all_topics = consumer.topics()
print(len(all_topics))

consumer.subscribe(topics=list(all_topics))
print(consumer.subscription())

for message in consumer:
    msg = message.value
    print(msg)
