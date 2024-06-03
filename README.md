CS 6350 Course Project: Analyzing Political Data on Reddit

Team:
Aniket Kulkarni (AMK190021)
Rhugaved Narmade (RRN210004)
Swetha Malaivaiyavur Elayavalli (SXM220052)
Hridya Dhulipala (HXZ220008)

Requirements:
1. Kafka kafka_2.13-3.7.0
2. Python 3.12.2
3. Spark 3.5.1 (spark-3.5.1-bin-hadoop3)

Steps:
0. Install required python packages
    pip3 install -r requirements.txt

1. Start the Kafka Environment
    a. Start ZooKeeper
        bin/zookeeper-server-start.sh config/zookeeper.properties
    b. Start the Kafka broker service
        bin/kafka-server-start.sh config/server.properties

2. Create kafka topics
    bin/kafka-topics.sh --create --topic reddit_producer --bootstrap-server localhost:9092
    bin/kafka-topics.sh --create --topic filtered_data_producer --bootstrap-server localhost:9092
    bin/kafka-topics.sh --create --topic reddit_hot_posts_producer --bootstrap-server localhost:9092
    bin/kafka-topics.sh --create --topic hot_posts_filtered_data_producer --bootstrap-server localhost:9092
    bin/kafka-topics.sh --create --topic user_engagement_analysis_producer --bootstrap-server localhost:9092
    bin/kafka-topics.sh --create --topic url_analysis_producer --bootstrap-server localhost:9092

4. Start producers & consumers
    consumers (in separate terminal tabs):
        spark-3.5.1-bin-hadoop3/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 ~/RedditAnalyzer/consumer1.py
        python3 ~/RedditAnalyzer/consumer2.py
        cd RedditAnalyzer && python3 sentiment.py
        spark-3.5.1-bin-hadoop3/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 ~/RedditAnalyzer/hot_posts_consumer1.py
        spark-3.5.1-bin-hadoop3/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 ~/RedditAnalyzer/hot_posts_consumer2.py
        python3 ~/RedditAnalyzer/hot_posts_consumer3.py
        python3 ~/RedditAnalyzer/hot_posts_consumer_3_2.py
        python3 ~/RedditAnalyzer/hot_posts_consumer_3_3.py

    producers (in separate terminal tabs):
        python3 ~/RedditAnalyzer/producer.py
        python3 ~/RedditAnalyzer/hot_posts_producer.py

5. Text summarization using GPT API
    ⁠Add the credentials for API_KEY, AZURE_ENDPOINT to openai_config.txt
    cd RedditAnalyzer
    python3 gpt_producer.py 

6. To train the SVM model:
    Download data from AWS S3: https://twitter-utd.s3.us-east-2.amazonaws.com/Twitter_Data.csv
    cd RedditAnalyzer
    python3 svm_model.py
