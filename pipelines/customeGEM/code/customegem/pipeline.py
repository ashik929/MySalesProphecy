from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from customegem.config.ConfigStore import *
from customegem.functions import *
from prophecy.utils import *
from customegem.graph import *

def pipeline(spark: SparkSession) -> None:
    df_silver_region = silver_region(spark)

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("customeGEM").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/customeGEM")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/customeGEM", config = Config)(pipeline)

if __name__ == "__main__":
    main()
