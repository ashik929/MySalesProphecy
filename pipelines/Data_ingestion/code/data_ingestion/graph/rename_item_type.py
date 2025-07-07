from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from data_ingestion.config.ConfigStore import *
from data_ingestion.functions import *

def rename_item_type(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from prophecy.utils.transpiler.dataframe_fcns import evaluate_expression

    return evaluate_expression(
        in0,
        userExpression = "regexp_replace(\"Item_Type\", '_', ' ')",
        selectedColumnNames = ["Item_Type"],
        sparkSession = spark
    )
