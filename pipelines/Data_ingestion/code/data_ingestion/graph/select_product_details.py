from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from data_ingestion.config.ConfigStore import *
from data_ingestion.functions import *

def select_product_details(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("product_id"), col("product_name"), col("category"), col("brand"), col("price"))
