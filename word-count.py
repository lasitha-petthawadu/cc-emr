import sys
from operator import add
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
conf = (SparkConf()
         .setAppName("HarryPotterWordCounter")
         .set("spark.executor.memory", "1g"))
sc = SparkContext(conf = conf)

print("Launching application..")
if __name__ == "__main__":
        print("Initiating main..")

        inputFile = "s3://cc-iit-emr/input-data/harry-potter.txt"
        print("Counting words in ", inputFile)
        lines = sc.textFile(inputFile)

        lines_nonempty = lines.filter( lambda x: len(x) > 0 )
        counts = lines_nonempty.flatMap(lambda x: x.split(' ')) \
                      .map(lambda x: (x, 1)) \
                      .reduceByKey(add)
        sqlContext = SQLContext(sc)
        df = sqlContext.createDataFrame(counts.coalesce(1, shuffle = True), ['word', 'count'])
        df.write.format("com.databricks.spark.csv").option("header", "true").save("s3://cc-iit-emr/output-data")
        sc.stop()