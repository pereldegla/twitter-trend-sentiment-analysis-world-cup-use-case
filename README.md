# Twitter trend sentiment analysis: A World Cup Use Case - France Football Team 

This project aims to analyze the sentiment of Twitter users towards the France football team during the World Cup. The tweets are collected using the Twitter API in both batch and streaming mode and processed using Apache Spark on Databricks. The processed data is stored in raw data tables. The whole merged data is then processed using a simple "when" function with two lists of words and the results are stored in refined tables for visualization and analysis.

This approach is 100% cloud and doesn't require any installation if you work with the same exact stack. For context, Spark and Kafka are a pain to install. The notebooks for the consumer, producer and batch_ingestion can be found in Notebooks folder so you can import them in your Databricks environment.

## Technologies Used
Python

Twitter API

Apache Spark on Databricks Community

Confluent Kafka
## Project Set-up
Clone the repository: git clone https://github.com/twitter-trend-sentiment-analysis-world-cup-use-case.git

Create a virtual environment and install the dependencies: pip install -r requirements.txt (if you are working on your machine)

Set up a Twitter API key by following the instructions here: https://developer.twitter.com/en/docs/twitter-api/getting-started/getting-access-to-the-twitter-api 

Set up a Databricks cluster and install the required libraries.

Set up a Confluent Kafka cluster.

## Running the project
Run the tweet collection script: python kafka_consumer.py

Run the Twitter API requests script: python kafka_producer.py

Run the batch processing script: python batch_ingestion.py

Run the analysis notebook: merge_and_sentiment_analysis.ipynb

## Additional Notes
The use of this can be at a much bigger scale : studying the behavior of football fans from different teams during a whole league duration and watching the score decrease as their team loses 5 home games in a row.

The project can also be further improved by using natural language processing techniques to better understand the sentiment of the tweets.

The project can also be extended to analyze the sentiment towards other teams or players during the World Cup.

This README was 80% written using ChatGPT
