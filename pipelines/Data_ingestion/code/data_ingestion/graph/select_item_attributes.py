from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from data_ingestion.config.ConfigStore import *
from data_ingestion.functions import *

def select_item_attributes(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("Item_Identifier"), 
        col("Item_Weight").cast(IntegerType()).alias("Item_Weight"), 
        col("Item_Fat_Content"), 
        col("Item_Visibility"), 
        lower(col("Item_Type")).alias("Item_Type"), 
        col("Item_MRP").cast(IntegerType()).alias("Item_MRP"), 
        col("Outlet_Identifier"), 
        col("Outlet_Establishment_Year"), 
        col("Outlet_Size"), 
        upper(col("Outlet_Location_Type")).alias("Outlet_Location_Type"), 
        col("Outlet_Type"), 
        col("Item_Outlet_Sales"), 
        (col("Item_Weight") * col("Item_MRP").cast(IntegerType())).alias("total_price"), 
        when(((col("Item_Weight").cast(IntegerType()) * col("Item_MRP").cast(IntegerType())) > 500), lit("high_price"))\
          .otherwise(lit("low_price"))\
          .alias("sales_catagory"), 
        concat(col("Outlet_Identifier"), lit(" - "), col("Outlet_Type")).alias("full_outlet"), 
        substring(col("Outlet_Identifier"), 4, 3).alias("outlet_number")
    )
