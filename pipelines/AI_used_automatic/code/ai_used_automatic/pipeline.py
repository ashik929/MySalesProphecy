from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from ai_used_automatic.config.ConfigStore import *
from ai_used_automatic.functions import *
from prophecy.utils import *

def pipeline(spark: SparkSession) -> None:
    pass

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("AI_used_automatic").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/AI_used_automatic")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/AI_used_automatic", config = Config)(pipeline)

if __name__ == "__main__":
    main()
