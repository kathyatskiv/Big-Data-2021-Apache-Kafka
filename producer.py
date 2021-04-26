from time import sleep
from json import dumps
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

producer = KafkaProducer(bootstrap_servers=['<brocker-1-id>:9092', '<brocker-2-id>:9092', '<brocker-3-id>:9092'],
                        value_serializer=lambda x: dumps(x).encode('utf-8'))

admin = KafkaAdminClient(
    bootstrap_servers=['<brocker-1-id>:9092', '<brocker-2-id>:9092', '<brocker-3-id>:9092'], 
    client_id='admin'
)

admin_consumer = KafkaConsumer(
     bootstrap_servers=['<brocker-1-id>:9092', '<brocker-2-id>:9092', '<brocker-3-id>:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group')

all_topics = admin_consumer.topics()
print(all_topics)

# topic_list = []
# topic_list.append(NewTopic(name="demo-topic-2", num_partitions=1, replication_factor=3))
# admin.create_topics(new_topics=topic_list, validate_only=False)


def produce(tweets) :
    for t in tweets:
        print(t['tweet_id'])
        print(t['text'])

        if t['author_id'] not in all_topics: 
            new_topic = NewTopic(name=t['author_id'], num_partitions=1, replication_factor=3)
            admin.create_topics(new_topics=[new_topic], validate_only=False)
            all_topics.add(t['author_id'])

        data = {
                'tweet_id' : t['tweet_id'],
                'created_at' : t['created_at'],
                'text' : t['text'],
                'response_tweet_id' : t['response_tweet_id'],
                'in_response_to_tweet_id' : t['in_response_to_tweet_id']
                }

        producer.send(t['author_id'], value=data)
    print("Total amount: ", len(tweets))