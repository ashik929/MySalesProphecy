from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from data_ingestion.config.ConfigStore import *
from data_ingestion.functions import *
from prophecy.utils import *
from data_ingestion.graph import *

def pipeline(spark: SparkSession) -> None:
    df_sample1 = sample1(spark)
    df_DynamicSelect_1 = DynamicSelect_1(spark)
    df_bronze_big_mart_1 = bronze_big_mart_1(spark)
    df_rename_item_type = rename_item_type(spark, df_bronze_big_mart_1)
    df_silver_products = silver_products(spark)
    df_normalize_account_schema = normalize_account_schema(spark, df_sample1)
    df_silver_orders = silver_orders(spark)
    df_sort_by_total_amount = sort_by_total_amount(spark, df_silver_orders)
    df_bronze_big_mart = bronze_big_mart(spark)
    df_select_item_attributes = select_item_attributes(spark, df_bronze_big_mart)
    df_aggregate_item_weight_by_location = aggregate_item_weight_by_location(spark, df_select_item_attributes)
    df_select_product_details = select_product_details(spark, df_silver_products)
    df_remove_duplicate_products = remove_duplicate_products(spark, df_select_product_details)
    df_filter_non_null_categories = filter_non_null_categories(spark, df_remove_duplicate_products)
    df_limit_to_four = limit_to_four(spark, df_normalize_account_schema)
    df_replace_item_type_spaces = replace_item_type_spaces(spark, df_rename_item_type)
    df_sample_random_rows = sample_random_rows(spark, df_replace_item_type_spaces)
    df_limit_to_twenty = limit_to_twenty(spark, df_sort_by_total_amount)
    df_restructure_order_data = restructure_order_data(spark, df_limit_to_twenty)
    df_silver_customers = silver_customers(spark)

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
