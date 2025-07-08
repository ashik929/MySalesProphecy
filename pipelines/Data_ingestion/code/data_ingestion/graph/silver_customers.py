from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from data_ingestion.config.ConfigStore import *
from data_ingestion.functions import *

def silver_customers(spark: SparkSession) -> DataFrame:
    return spark.read.table("`project_catalog`.`silver`.`customers`")
