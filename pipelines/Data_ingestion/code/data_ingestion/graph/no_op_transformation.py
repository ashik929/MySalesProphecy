from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from data_ingestion.config.ConfigStore import *
from data_ingestion.functions import *

def no_op_transformation(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("Item_MRP", col("Item_MRP").cast(DoubleType()))\
        .withColumn("Item_Outlet_Sales", col("Item_Outlet_Sales").cast(DoubleType()))\
        .withColumn("Item_Visibility", col("Item_Visibility").cast(DoubleType()))\
        .withColumn("Item_Weight", col("Item_Weight").cast(DoubleType()))\
        .withColumn("Outlet_Establishment_Year", col("Outlet_Establishment_Year").cast(IntegerType()))
