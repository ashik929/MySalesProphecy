from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from join.config.ConfigStore import *
from join.functions import *

def left_outer_item_join(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .hint("broadcast")\
        .join(in1.alias("in1"), (col("in0.Item_Identifier") == col("in1.Item_Identifier")), "outer")\
        .select(*[expr("ln(CAST(in0.Item_Outlet_Sales AS DOUBLE))").alias("log_sales")], col("in0.*"))
