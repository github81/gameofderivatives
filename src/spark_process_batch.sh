#!/bin/bash
SPARK_MASTER=ip-10-0-0-6.ec2.internal
cassandra_host='ip-10-0-0-4'
cassandra_keyspace='swappg'
redis_host='ip-10-0-0-12'
hdfs_input_dir='<public-dns>:9000'
spark-submit --master spark://${SPARK_MASTER}:7077 --conf spark.cassandra.connection.host=$CASSANDRA_WORKERS --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,datastax:spark-cassandra-connector:2.0.5-s_2.11 process_batch.py ${cassandra_host} ${cassandra_keyspace} ${hdfs_input_dir} ${redis_host}