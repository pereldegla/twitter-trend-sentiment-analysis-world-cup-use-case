import sys
from confluent_kafka import avro, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from uuid import uuid4
import tweepy
import time
from pyspark.sql.types import *
from confluent_kafka import Producer, KafkaError
import json
from datetime import datetime
import datetime as d
import random
import os

## KAFKA
CONSUMER_KEY = ""
CONSUMER_SECRET = ""
ACCESS_TOKEN_KEY = ""
ACCESS_TOKEN_SECRET = ""

## TWITTER
api_key = ""
api_secret = ""
bearer_token = ""
access_token = ""
access_token_secret = ""

# Gainaing access and connecting to Twitter API using Credentials
client = tweepy.Client(bearer_token, api_key, api_secret, access_token, access_token_secret)

auth = tweepy.OAuth1UserHandler(api_key, api_secret, access_token, access_token_secret)
api = tweepy.API(auth)

def create_topic(conf, topic):
    """
        Create a topic if needed
        Examples of additional admin API functionality:
        https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/adminapi.py
    """

    admin_client_conf = conf.copy()
    a = AdminClient(admin_client_conf)

    fs = a.create_topics([NewTopic(
         topic,
         num_partitions=1,
         replication_factor=3
    )])
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} created".format(topic))
        except Exception as e:
            # Continue if error code TOPIC_ALREADY_EXISTS, which may be true
            # Otherwise fail fast
            if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                print("Failed to create topic {}: {}".format(topic, e))
                sys.exit(1)

if __name__ == "__main__":
    # Read arguments and configurations and initialize
    topic = "topic_0"
    conf = {'bootstrap.servers':'',
            'security.protocol':'SASL_SSL',
           'sasl.mechanisms':'PLAIN',
            'sasl.username':'',
            'sasl.password':''}   
    
    # Create Producer instance
    producer = Producer(conf)
    # Create topic if needed
    create_topic(conf, topic)
    
    class MyStream(tweepy.StreamingClient):
        # This function gets called when the stream is working
        def on_connect(self):
            print("Connected")

        def on_tweet(self, tweet):
            """
            This function gets called when a tweet passes through the stream 
            and it checks if it respect the rules
            then serializes the value and publish a msg to the topic
            """
            
            query = '(#FRAMAR OR #MARFRA OR Mbappe) -is:retweet lang:fr -has:media' #set according to Twitter API - TO BE UPDATED 
			
            count = 0
            switch = True
            while switch:
                tic = datetime.now() 
                for tweet in tweepy.Paginator(client.search_recent_tweets,query=query,tweet_fields=['id','created_at'],start_time="2022-12-14T11:32:12.610841+00:00", max_results=10).flatten(limit=20):
                    if tweet.referenced_tweets == None:
                        if tweet.text.count('#') < 2 and tweet.id is not None:  
                            time.sleep(0.5) # Delay between tweets
                            record_key = str(tweet.id)
                            record_value = json.dumps({"id":str(tweet.id), "tweet": str(tweet), "created_at": str(tweet.created_at)}) #create new json record
                            #print(record_value) #uncomment to display record i console
							partition = random.randint(1, 5) #balance the partition usage
                            producer.produce(
                                topic=topic,
                                partition=partition,
                                key = record_key,
                                value=record_value,
                                )                                                       
                            count +=1                            
                            producer.poll(0)
                            if count == 20: #flush after 20 tweets 
                                count = 0
                                switch = False
            tac = datetime.now()
            producer.flush()
            elapsed_time = tac - tic
            print("flushed after:", elapsed_time) # Use it to adjust the consumer processing rate
    
    stream = MyStream(bearer_token)
    stream.filter(tweet_fields=["referenced_tweets"]) #start stream
