from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from data_ingestion.config.ConfigStore import *
from data_ingestion.functions import *

def replace_item_type_spaces(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        *(
          [expr("regexp_replace(`Item Type`, ' ', '_')\r\n").alias("Item Type")]
          + [col("`" + colName + "`") for colName in sorted(set(in0.columns) - {"Item Type"})]
          + []
        )
    )
