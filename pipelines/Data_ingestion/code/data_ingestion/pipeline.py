from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from data_ingestion.config.ConfigStore import *
from data_ingestion.functions import *
from prophecy.utils import *
from data_ingestion.graph import *

def pipeline(spark: SparkSession) -> None:
    df_ds_source = ds_source(spark)
    df_ds_adls_source = ds_adls_source(spark)

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("Data_ingestion").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/Data_ingestion")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/Data_ingestion", config = Config)(pipeline)

if __name__ == "__main__":
    main()
