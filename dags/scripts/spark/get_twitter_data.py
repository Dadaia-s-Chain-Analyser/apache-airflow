from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
sc = SparkContext()

ssc = StreamingContext(sc, 10)
socket_stream = ssc.socketTextStream('airflowcd', 5555)
lines = socket_stream.window(20)

from collections import namedtuple

fields = ('tags', 'count')
Tweet = namedtuple('Tweet', fields)

(   lines.flatMap(lambda text: text.split(" "))
    .filter(lambda word: word.lower().startswith('#'))
    .map(lambda word: (word.lower(), 1))
    .reduceByKey(lambda a, b: a + b)
    .map(lambda rec: Tweet(rec[0], rec[1]))
    .foreachRDD(lambda rdd: rdd.toDF().sort(desc('count'))
    .limit(10).registerTempTable('tweets')))
