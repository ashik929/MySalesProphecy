from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from data_ingestion.config.ConfigStore import *
from data_ingestion.functions import *

def aggregate_item_weight_by_location(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("Outlet_Location_Type"), col("Item_Fat_Content"))
    df2 = df1.pivot("Outlet_Location_Type")

    return df2.agg(sum(col("Item_Weight")).alias("Item_Weight"))
