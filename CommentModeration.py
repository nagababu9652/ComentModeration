from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
# import pyspark.sql.functions as F
from lib.logger import Log4j
import findspark
import re
import demoji
import nltk
# import uuid

nltk.download('punkt')
nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer

findspark.init('/home/naga/Downloads/spark-3.1.3-bin-hadoop3.2')
findspark.add_packages(
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0")


def start(string):
    if string is None:
        return "NO-comment"
    else:
        string = re.sub('<.*?>', '', string)
        reglink = r'http\S+|www.\S+'
        string = re.sub(reglink, " ", string)
        string = demoji.replace(string, "")
        regex = r"[^0-9A-Za-z'\t]"
        rx = r"[a-zA-Z0-9]*[0-9][a-zA-Z0-9]*"
        rxspace = r" +"
        string = re.sub(regex, " ", string)
        string = re.sub(rx, "", string)
        string = re.sub(rxspace, " ", string).lower()
        sid = SentimentIntensityAnalyzer()
        score = sid.polarity_scores(string)
        polarity = score["compound"]
        behaviour = ""
        if polarity > 0:
            behaviour = "Positive"
        elif polarity < 0:
            behaviour = "Negative"
        elif polarity == 0:
            behaviour = "Neutral"
        return behaviour


def writeToCassandra(writeDF, idDf):
    writeDF.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace="test", table="commentresult") \
        .mode("append") \
        .save()


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("HelloSparkSQL") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()

    logger = Log4j(spark)

    commentDF = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "comments") \
        .option("startingOffsets", "earliest") \
        .load()

    schema = StructType([StructField("CommentId", StringType()),
                         StructField("CreatorId", StringType()),
                         StructField("Comment", StringType())
                         ])

    value_df = commentDF.select(from_json(commentDF.value.cast("string"), schema).alias("values"))

    filter_df = value_df.select("values.CommentId", "values.CreatorId", "values.Comment")

    my_udf = spark.udf.register("_nullsafeUDF", lambda stri: start(stri), StringType())
    Process_df = filter_df.select(col("CommentId"), col("CreatorId"), col("Comment"),
                                  my_udf(col("Comment")).alias("Polarity"))

    kafka_target_df = Process_df.selectExpr("""to_json(named_struct(
                                            'CommentId', CommentId,
                                            'CreatorId', CreatorId,
                                            'Comment', Comment,
                                            'Polarity', Polarity)) as value
                                            """)
    KafkaQuery = kafka_target_df.writeStream \
        .option("checkpointLocation", 'tmp/check_point/') \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "commentsResult") \
        .outputMode(outputMode="append") \
        .start()

    For_Cass_df = filter_df.select(col("CommentId").alias("commentid"),
                                   col("Comment").alias("comment"),
                                   my_udf(col("Comment")).alias("polarity"))

    # For_Cass_df = For_Cass_df.withColumn('commentid', F.when(For_Cass_df['commentid'].isNull(), "00000000-0000-0000-0000-000000000000").otherwise(col('commentid')))

    CassandraQuery = For_Cass_df.writeStream \
        .option("spark.cassandra.connection.host", "localhost:9042") \
        .foreachBatch(writeToCassandra) \
        .option("checkpointLocation", 'tmp/check_point1/') \
        .outputMode(outputMode="update") \
        .start()

    spark.streams.awaitAnyTermination()
