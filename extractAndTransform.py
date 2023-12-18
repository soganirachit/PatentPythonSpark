import os

from pyspark.sql.functions import split, regexp_replace
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql import SparkSession

os.environ["HADOOP_HOME"] = "C:/spark-3.5.0-bin-hadoop3/"

spark = SparkSession.builder.master("local[*]") \
    .appName('TestOracleJdbc') \
    .config("spark.jars",
            r"C:\Users\rachit sogani\.m2\repository\com\oracle\database\jdbc\ojdbc8\21.11.0.0\ojdbc8-21.11.0.0.jar") \
    .getOrCreate()
spark.conf.set("spark.sql.oracle.enabled", "true")

query = "SELECT INVENTION_TITLE,APPLICATION_NUMBER, FIELD_OF_INVENTION, ABSTRACT_OF_INVENTION, COMPLETE_SPECIFICATION FROM patent_details"

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@localhost:1521:orcl") \
    .option("driver", "oracle.jdbc.OracleDriver") \
    .option("query", query) \
    .option("user", "sys as sysdba") \
    .option("password", "sys123") \
    .load()

df = df.withColumn("INVENTION_TITLE_SpecialCharsRemoved", regexp_replace(df["INVENTION_TITLE"], "[^a-zA-Z0-9\\s]", ""))
df = df.withColumn("INVENTION_TITLE_ARRAY", split(df["INVENTION_TITLE_SpecialCharsRemoved"], " "))
remover = StopWordsRemover(inputCol="INVENTION_TITLE_ARRAY", outputCol="INVENTION_TITLE_CLEANED")
print()
remover.setStopWords(remover.getStopWords() + ["using", "use", "based"])
df1 = remover.transform(df)

print("StopWordsRemover output is")
df1.select("INVENTION_TITLE", "INVENTION_TITLE_CLEANED").show(15)
df1.repartition(10,"FIELD_OF_INVENTION").select("APPLICATION_NUMBER", "INVENTION_TITLE", "INVENTION_TITLE_CLEANED").write.json(
    r"E:\CaptchaImages\NewDownload\df1.json")
# df1.repartition(10,"FIELD_OF_INVENTION").select("APPLICATION_NUMBER", "INVENTION_TITLE", "INVENTION_TITLE_CLEANED").write.parquet(
#     r"E:\CaptchaImages\NewDownload\df1.parquet")
