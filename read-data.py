import pandas as pd
import numpy as np

from producer import produce

def convert(x):
    arr = np.array(x.split(' '))
    months = {
        "Jan" : 1,
        "Feb" : 2,
        "Mar" : 3,
        "Apr" : 4,
        "May" : 5,
        "Jun" : 6,
        "Jul" : 7,
        "Aug" : 8,
        "Sep" : 9,
        "Oct" : 10,
        "Nov" : 11,
        "Dec" : 12
    }

    datetime = str(arr[5]) + "-" + str(months[arr[1]]) + "-" + str(arr[2]) + " " + arr[3]
    return datetime


df = pd.read_csv("twcs.csv")

# Data preprocessing

df = df[df['response_tweet_id'].notna()&df['in_response_to_tweet_id'].notna()]
df = df.astype({"in_response_to_tweet_id": np.int64})
df['created_at'] = df['created_at'].apply(lambda x: convert(x))
df['created_at'] = pd.to_datetime(df['created_at'], format='%Y-%m-%d %H:%M:%S')
df = df.sort_values(by='created_at')


print(df.dtypes)
print(df.tail())

chunk_size = 35
for start in range(71, df.size, chunk_size):
    df_subset = df.iloc[start:start + chunk_size]
    df_subset['created_at'] = df_subset['created_at'].astype(str)
    produce(df_subset.to_dict('records'))

