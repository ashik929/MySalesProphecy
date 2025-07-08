from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from data_ingestion.config.ConfigStore import *
from data_ingestion.functions import *

def restructure_order_data(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("order_id"), 
        col("customer_id"), 
        col("product_id"), 
        col("order_date"), 
        col("quantity"), 
        col("total_amount")
    )
