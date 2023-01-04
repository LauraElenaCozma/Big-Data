import pyspark as ps
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import desc
import pyspark.sql.types as tp

sc = SparkContext.getOrCreate()
conf = ps.SparkConf().setMaster("yarn-client").setAppName("sparK-mer")
conf.set("spark.executor.heartbeatInterval","3600s")
print( "spark version=" ,SparkSession.builder.getOrCreate().version)

ssc = StreamingContext(sc, 10 )
sqlContext = SQLContext(sc)
socket_stream = ssc.socketTextStream("localhost", 5554)

# Se divide fiecare linie în cuvinte
words = socket_stream.flatMap(lambda line: line.split(" "))

# Se numără cuvintele din fiecare batch
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Se afișează primele 10 elemente ale fiecărui RDD generat în DStream la consolă
wordCounts.pprint()

ssc.start()
ssc.awaitTerminationOrTimeout(100)