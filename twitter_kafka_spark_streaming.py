import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

def tweetWordCount(kvs):
    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: json.loads(line)["text"].split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
    sys.stdout.flush()
    return counts

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print "Usage: kafka_wordcount.py <zk> <topic>"
        exit(-1)

    sc = SparkContext(appName="PythonStreamingKafkaWordCount")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 60)

    zkQuorum, topic = sys.argv[1:]
    kafkaParams = { "zookeeper.connection.timeout.ms":"60000"}
    kvs = KafkaUtils.createStream(ssc,zkQuorum, "spark-streaming-consumer", {topic: 1},kafkaParams)
    counts = tweetWordCount(kvs)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()