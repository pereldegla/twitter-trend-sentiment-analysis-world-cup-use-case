import tweepy
import time
from pyspark.sql.types import *
import json
from datetime import datetime 
from pyspark.sql import Row
from pyspark.sql.functions import to_timestamp, col, to_date, date_sub

api_key = ""
api_secret = ""
bearer_token = ""
access_token = ""
access_token_secret = ""

# Gainaing access and connecting to Twitter API using Credentials
client = tweepy.Client(bearer_token, api_key, api_secret, access_token, access_token_secret)

auth = tweepy.OAuth1UserHandler(api_key, api_secret, access_token, access_token_secret)

api = tweepy.API(auth,wait_on_rate_limit=True)

query = '(Mbappe) -is:retweet lang:fr -has:media' #set according to Twitter API - TO BE UPDATED 

tweet_list= []
outputPath = "/batch_location"

for tweet in tweepy.Paginator(client.search_recent_tweets,query=query,start_time="2022-12-19T08:00:12.610841+00:00", tweet_fields=['id','created_at'], max_results=100).flatten(limit=40000): #,start_time="2022-12-14T19:32:12.610841+00:00"
    if tweet.referenced_tweets == None:
        if tweet.text.count('#') < 2 :
            tweet_list.append({"id":str(tweet.id), "tweet": str(tweet), "created_at": str(tweet.created_at)})

tweet_df = spark.createDataFrame(tweet_list)
tweet_df = (tweet_df
            .withColumn("id", col("id").cast('long'))
            .withColumn("created_at", to_timestamp(col("created_at")))
            .dropDuplicates()
           )
(tweet_df
 .write
 .format("delta")
 .mode("append") #append le 06/12
 .save(outputPath)
)


#print(tweet_df.count())

# COMMAND ----------

(spark
 .read
 .format('delta')
 .load(outputPath)
 .dropDuplicates()
 .write
 .format("delta")
 .mode("overwrite") 
 .save(outputPath)
)


# COMMAND ----------

(spark
 .read
 .format('delta')
 .load(outputPath)
 .count()
)
