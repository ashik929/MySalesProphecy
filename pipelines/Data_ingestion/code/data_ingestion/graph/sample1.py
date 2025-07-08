from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from data_ingestion.config.ConfigStore import *
from data_ingestion.functions import *

def sample1(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("json")\
        .schema(
          StructType([
            StructField("account", StructType([
              StructField("created", StringType(), True), StructField("status", StringType(), True), StructField("subscription", StructType([
                StructField("renewal", StructType([
                  StructField("autoRenew", BooleanType(), True), StructField("nextBillingDate", StringType(), True)
                ]), True), StructField("type", StringType(), True)
              ]), True)
            ]), True), StructField("login", StructType([
              StructField("ip", StringType(), True), StructField("location", StructType([
                StructField("city", StringType(), True), StructField("country", StringType(), True)
              ]), True), StructField("timestamp", StringType(), True)
            ]), True), StructField("paymentMethod", StructType([
              StructField("card", StructType([
                StructField("brand", StringType(), True), StructField("expiry", StructType([
                  StructField("month", LongType(), True), StructField("year", LongType(), True)
                ]), True), StructField("last4", StringType(), True)
              ]), True), StructField("email", StringType(), True), StructField("type", StringType(), True)
            ]), True), StructField("purchase", StructType([
              StructField("amount", DoubleType(), True), StructField("currency", StringType(), True), StructField("date", StringType(), True), StructField("id", StringType(), True), StructField("items", ArrayType(
              StructType([
                StructField("name", StringType(), True), StructField("qty", LongType(), True), StructField("sku", StringType(), True)
            ]), 
              True
          ), True)
            ]), True), StructField("type", StringType(), True), StructField("user", StructType([
              StructField("contact", StructType([
                StructField("address", StructType([
                  StructField("city", StringType(), True), StructField("location", StructType([
                    StructField("lat", DoubleType(), True), StructField("lng", DoubleType(), True)
                  ]), True), StructField("state", StringType(), True), StructField("street", StringType(), True), StructField("zip", StringType(), True)
                ]), True), StructField("phone", StructType([
                  StructField("home", StringType(), True), StructField("work", StringType(), True)
                ]), True)
              ]), True), StructField("email", StringType(), True), StructField("id", StringType(), True), StructField("name", StructType([
                StructField("first", StringType(), True), StructField("last", StringType(), True), StructField("middle", StringType(), True)
              ]), True), StructField("preferences", StructType([
                StructField("notifications", StructType([
                  StructField("email", BooleanType(), True), StructField("push", StructType([
                    StructField("enabled", BooleanType(), True), StructField("frequency", StringType(), True)
                  ]), True), StructField("sms", BooleanType(), True)
                ]), True), StructField("theme", StringType(), True)
              ]), True)
            ]), True)
        ])
        )\
        .load("dbfs:/sample1.json")
