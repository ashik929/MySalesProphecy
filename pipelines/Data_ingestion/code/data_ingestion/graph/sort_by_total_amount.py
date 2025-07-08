from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from data_ingestion.config.ConfigStore import *
from data_ingestion.functions import *

def sort_by_total_amount(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.orderBy(col("total_amount").asc_nulls_last(), col("customer_id").asc_nulls_last())
