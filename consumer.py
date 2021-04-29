import json
from json import loads
import datetime 

from kafka import KafkaConsumer, TopicPartition


data = {}

def main():
    consumer = KafkaConsumer(
     bootstrap_servers=['<broker_1_id>:9092', '<broker_2_id>:9092', '<broker_3_id>:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True)

    resubscribe(consumer)

    try:
        all_accounts = get_all_acounts(consumer)

        data['accounts'] = []
        data['accounts'].append(list(all_accounts))
    except Exception as e:
        print("get_all_acounts")
        print(e)
    finally:
        print("get_all_acounts loaded")
        print(data['accounts'])



   
    get_tweets_for_top_10_accounts(consumer)    
    print("get_tweets_for_top_10_accounts loaded")

    try:
        get_top_20_accounts(consumer,1)
    except Exception as e:
        print("get_top_20_accounts")
        print(e)
    finally:
        print("get_top_20_accounts loaded")
        print(data['top20_producing_accounts'])

    try:
        get_aggregated_statistics(consumer)
    except Exception as e:
        print("get_aggregated_statistics")
        print(e)
    finally:
        print("get_aggregated_statistics loaded")
        print(data['aggregated_statistics'])

    with open('data.json', 'w') as outfile:
        json.dump(data, outfile)

    consumer.close()


def resubscribe(consumer):
    all_topics = consumer.topics()
    consumer.subscribe(topics=list(all_topics))


def get_all_acounts(consumer):
    all_topics = consumer.topics()

    return all_topics


def get_tweets_for_top_10_accounts(consumer):
    resubscribe(consumer)
    all_topics = get_all_acounts(consumer)

    top10 = []
    start_date = datetime.datetime.timestamp(datetime.datetime.now() - datetime.timedelta(hours=3))

    for topic in all_topics:
        tp = TopicPartition(topic, 0)
        consumer.seek_to_end(tp)
        last_offset = consumer.position(tp)

        offset = consumer.offsets_for_times({tp: start_date})

        start_offset = 0
        if list(offset.values())[0]:
            start_offset = list(offset.values())[0].offset
            

        top10.append({'topic' : topic, 'length' : last_offset - start_offset})


    sorted(top10, key = lambda x: top10['length'])
    top10 = top10[:10]
    print(top10)

def get_top_20_accounts(consumer, n):
    resubscribe(consumer)
    all_topics = get_all_acounts(consumer)

    top20 = []
    start_date = datetime.datetime.timestamp(datetime.datetime.now() - datetime.timedelta(hours=n))

    for topic in all_topics:
        tp = TopicPartition(topic, 0)
        consumer.seek_to_end(tp)
        last_offset = consumer.position(tp)

        offset = consumer.offsets_for_times({tp: start_date})

        start_offset = 0
        if list(offset.values())[0]:
            start_offset = list(offset.values())[0].offset
            

        top20.append({'topic' : topic, 'length' : last_offset - start_offset})


    sorted(top20, key = lambda x: top20['length'])
    top20 = top20[:20]
    
    data["top20_producing_accounts"] = top20

def get_aggregated_statistics(consumer):
    resubscribe(consumer)
    all_topics = get_all_acounts(consumer)
    data['aggregated_statistics'] = []

    hour_before_1 = datetime.datetime.timestamp(datetime.datetime.now() - datetime.timedelta(hours=1))
    hour_before_2 = datetime.datetime.timestamp(datetime.datetime.now() - datetime.timedelta(hours=1))
    hour_before_3 = datetime.datetime.timestamp(datetime.datetime.now() - datetime.timedelta(hours=3))

    for topic in all_topics:
        tp = TopicPartition(topic, 0)
        consumer.seek_to_end(tp)
        last_offset = consumer.position(tp)

        offset_1_hour_before = consumer.offsets_for_times({tp: hour_before_1})
        offset_2_hour_before = consumer.offsets_for_times({tp: hour_before_2})
        offset_3_hour_before = consumer.offsets_for_times({tp: hour_before_3})

        start_minus_1 = 0
        start_minus_2 = 0
        start_minus_3 = 0

        if list(offset_1_hour_before.values())[0]:
            start_minus_1 = list(offset_1_hour_before.values())[0].offset

        if list(offset_2_hour_before.values())[0]:
            start_minus_2 = list(offset_2_hour_before.values())[0].offset

        if list(offset_3_hour_before.values())[0]:
            start_minus_3 = list(offset_3_hour_before.values())[0].offset

        data['aggregated_statistics'].append(
            {
                'topic' : topic, 
                '1 hour before' : last_offset - start_minus_1, 
                '2 hours before' : start_minus_1 - start_minus_2,
                '3 hour before' : start_minus_2 - start_minus_3
            })
        
    

if __name__ == "__main__":
    main()



