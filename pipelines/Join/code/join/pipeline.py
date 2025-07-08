from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from join.config.ConfigStore import *
from join.functions import *
from prophecy.utils import *
from join.graph import *

def pipeline(spark: SparkSession) -> None:
    df_ds_join_sales = ds_join_sales(spark)
    df_ds_join_items = ds_join_items(spark)
    df_left_outer_item_join = left_outer_item_join(spark, df_ds_join_sales, df_ds_join_items)
    df_repartition_data = repartition_data(spark, df_left_outer_item_join)

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("Join").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/Join")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/Join", config = Config)(pipeline)

if __name__ == "__main__":
    main()
