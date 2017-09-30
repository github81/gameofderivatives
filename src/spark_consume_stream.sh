#!/bin/bash
topic=$1
KAFKA_PRODUCER_HOST=$2
SPARK_CONSUMER_HOST=$3
REDIS_CONSUMER_HOST='<public-dns>'
CASSANDRA_HOST=ip-10-0-0-5
hdfs_ir_input_dir='hdfs://ec2-34-226-0-118.compute-1.amazonaws.com:9000'
app_name=$4
spark-submit --master spark://ip-${SPARK_CONSUMER_HOST}:7077 --conf spark.cassandra.connection.host=$CASSANDRA_HOST --conf spark.yarn.submit.waitAppCompletion=false --conf spark.streaming.backpressure.enabled=true --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,datastax:spark-cassandra-connector:2.0.5-s_2.11 consume_stream.py ip-${KAFKA_PRODUCER_HOST} ${REDIS_CONSUMER_HOST} ${topic} ${hdfs_ir_input_dir} ${app_name}

#spark-submit --master spark://ip-${SPARK_CONSUMER_HOST}:7077 --conf spark.cassandra.connection.host=$CASSANDRA_HOST --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,datastax:spark-cassandra-connector:2.0.5-s_2.11 consume_stream.py ip-${KAFKA_PRODUCER_HOST} ${REDIS_CONSUMER_HOST} ${topic} ${hdfs_ir_input_dir} ${app_name}
