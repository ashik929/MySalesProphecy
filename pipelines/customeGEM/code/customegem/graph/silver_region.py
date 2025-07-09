from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from customegem.config.ConfigStore import *
from customegem.functions import *

def silver_region(spark: SparkSession) -> DataFrame:
    return spark.read.table("`project_catalog`.`silver`.`region`")
