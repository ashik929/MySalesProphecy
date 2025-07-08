from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from data_ingestion.config.ConfigStore import *
from data_ingestion.functions import *

def sample_random_rows(spark: SparkSession, in0: DataFrame) -> DataFrame:
    import random
    totalRows = in0.count()

    if totalRows == 0 or totalRows <= 12:
        return in0

    df1 = in0.orderBy(*in0.columns)

    return df1\
        .withColumn("row_num", row_number().over(Window.orderBy(*in0.columns)))\
        .withColumn("hash_val", sha2(concat(col("row_num"), lit(str(random.randint(1, totalRows)))), 256))\
        .orderBy("hash_val")\
        .limit(12)\
        .drop("row_num", "hash_val")
