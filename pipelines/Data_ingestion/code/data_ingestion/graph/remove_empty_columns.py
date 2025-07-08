from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from data_ingestion.config.ConfigStore import *
from data_ingestion.functions import *

def remove_empty_columns(spark: SparkSession, df: DataFrame) -> DataFrame:
    from pyspark.sql.functions import col, trim, regexp_replace, lower, upper, initcap
    from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, LongType, ShortType
    # Step 2: Apply data cleansing operations
    # Start with the original columns
    transformed_columns = []

    # Check if column exists after null operations
    if "Item Type" not in df.columns:
        print("Warning: Column 'Item Type' not found after null operation. Skipping transformations for this column.")
    else:
        col_type = df.schema["Item Type"].dataType

        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["Item Type"].dataType, StringType):
            df = df.na.fill({"Item Type" : "NA"})
            transformed_columns = [col("Item Type")]
        elif isinstance(col_type, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            transformed_columns = [col("Item Type")]
        else:
            transformed_columns = [col("Item Type")]

    # Check if column exists after null operations
    if "Item_Fat_Content" not in df.columns:
        print(
            "Warning: Column 'Item_Fat_Content' not found after null operation. Skipping transformations for this column."
        )
    else:
        col_type = df.schema["Item_Fat_Content"].dataType

        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["Item_Fat_Content"].dataType, StringType):
            df = df.na.fill({"Item_Fat_Content" : "NA"})
            # Add the transformed column to the list with alias
            transformed_columns.append(col("Item_Fat_Content"))
        elif isinstance(col_type, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            transformed_columns.append(col("Item_Fat_Content"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Item_Fat_Content"))

    # Check if column exists after null operations
    if "Item_Identifier" not in df.columns:
        print(
            "Warning: Column 'Item_Identifier' not found after null operation. Skipping transformations for this column."
        )
    else:
        col_type = df.schema["Item_Identifier"].dataType

        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["Item_Identifier"].dataType, StringType):
            df = df.na.fill({"Item_Identifier" : "NA"})
            # Add the transformed column to the list with alias
            transformed_columns.append(col("Item_Identifier"))
        elif isinstance(col_type, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            transformed_columns.append(col("Item_Identifier"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Item_Identifier"))

    # Check if column exists after null operations
    if "Item_MRP" not in df.columns:
        print("Warning: Column 'Item_MRP' not found after null operation. Skipping transformations for this column.")
    else:
        col_type = df.schema["Item_MRP"].dataType

        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["Item_MRP"].dataType, StringType):
            df = df.na.fill({"Item_MRP" : "NA"})
            # Add the transformed column to the list with alias
            transformed_columns.append(col("Item_MRP"))
        elif isinstance(col_type, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            transformed_columns.append(col("Item_MRP"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Item_MRP"))

    # Check if column exists after null operations
    if "Item_Outlet_Sales" not in df.columns:
        print(
            "Warning: Column 'Item_Outlet_Sales' not found after null operation. Skipping transformations for this column."
        )
    else:
        col_type = df.schema["Item_Outlet_Sales"].dataType

        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["Item_Outlet_Sales"].dataType, StringType):
            df = df.na.fill({"Item_Outlet_Sales" : "NA"})
            # Add the transformed column to the list with alias
            transformed_columns.append(col("Item_Outlet_Sales"))
        elif isinstance(col_type, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            transformed_columns.append(col("Item_Outlet_Sales"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Item_Outlet_Sales"))

    # Check if column exists after null operations
    if "Item_Visibility" not in df.columns:
        print(
            "Warning: Column 'Item_Visibility' not found after null operation. Skipping transformations for this column."
        )
    else:
        col_type = df.schema["Item_Visibility"].dataType

        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["Item_Visibility"].dataType, StringType):
            df = df.na.fill({"Item_Visibility" : "NA"})
            # Add the transformed column to the list with alias
            transformed_columns.append(col("Item_Visibility"))
        elif isinstance(col_type, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            transformed_columns.append(col("Item_Visibility"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Item_Visibility"))

    # Check if column exists after null operations
    if "Item_Weight" not in df.columns:
        print(
            "Warning: Column 'Item_Weight' not found after null operation. Skipping transformations for this column."
        )
    else:
        col_type = df.schema["Item_Weight"].dataType

        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["Item_Weight"].dataType, StringType):
            df = df.na.fill({"Item_Weight" : "NA"})
            # Add the transformed column to the list with alias
            transformed_columns.append(col("Item_Weight"))
        elif isinstance(col_type, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            transformed_columns.append(col("Item_Weight"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Item_Weight"))

    # Check if column exists after null operations
    if "Outlet_Establishment_Year" not in df.columns:
        print(
            "Warning: Column 'Outlet_Establishment_Year' not found after null operation. Skipping transformations for this column."
        )
    else:
        col_type = df.schema["Outlet_Establishment_Year"].dataType

        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["Outlet_Establishment_Year"].dataType, StringType):
            df = df.na.fill({"Outlet_Establishment_Year" : "NA"})
            # Add the transformed column to the list with alias
            transformed_columns.append(col("Outlet_Establishment_Year"))
        elif isinstance(col_type, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            transformed_columns.append(col("Outlet_Establishment_Year"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Outlet_Establishment_Year"))

    # Check if column exists after null operations
    if "Outlet_Identifier" not in df.columns:
        print(
            "Warning: Column 'Outlet_Identifier' not found after null operation. Skipping transformations for this column."
        )
    else:
        col_type = df.schema["Outlet_Identifier"].dataType

        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["Outlet_Identifier"].dataType, StringType):
            df = df.na.fill({"Outlet_Identifier" : "NA"})
            # Add the transformed column to the list with alias
            transformed_columns.append(col("Outlet_Identifier"))
        elif isinstance(col_type, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            transformed_columns.append(col("Outlet_Identifier"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Outlet_Identifier"))

    # Check if column exists after null operations
    if "Outlet_Location_Type" not in df.columns:
        print(
            "Warning: Column 'Outlet_Location_Type' not found after null operation. Skipping transformations for this column."
        )
    else:
        col_type = df.schema["Outlet_Location_Type"].dataType

        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["Outlet_Location_Type"].dataType, StringType):
            df = df.na.fill({"Outlet_Location_Type" : "NA"})
            # Add the transformed column to the list with alias
            transformed_columns.append(col("Outlet_Location_Type"))
        elif isinstance(col_type, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            transformed_columns.append(col("Outlet_Location_Type"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Outlet_Location_Type"))

    # Check if column exists after null operations
    if "Outlet_Size" not in df.columns:
        print(
            "Warning: Column 'Outlet_Size' not found after null operation. Skipping transformations for this column."
        )
    else:
        col_type = df.schema["Outlet_Size"].dataType

        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["Outlet_Size"].dataType, StringType):
            df = df.na.fill({"Outlet_Size" : "NA"})
            # Add the transformed column to the list with alias
            transformed_columns.append(col("Outlet_Size"))
        elif isinstance(col_type, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            transformed_columns.append(col("Outlet_Size"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Outlet_Size"))

    # Check if column exists after null operations
    if "Outlet_Type" not in df.columns:
        print(
            "Warning: Column 'Outlet_Type' not found after null operation. Skipping transformations for this column."
        )
    else:
        col_type = df.schema["Outlet_Type"].dataType

        # If the column is a string type, apply text-based operations
        if isinstance(df.schema["Outlet_Type"].dataType, StringType):
            df = df.na.fill({"Outlet_Type" : "NA"})
            # Add the transformed column to the list with alias
            transformed_columns.append(col("Outlet_Type"))
        elif isinstance(col_type, (IntegerType, FloatType, DoubleType, LongType, ShortType)):
            transformed_columns.append(col("Outlet_Type"))
        else:
            # If the column doesn't require transformation, add it as is
            transformed_columns.append(col("Outlet_Type"))

    df = df.select(
        *[
          col(c)
          for c in df.columns
          if (
          c
          not in ["Item Type",  "Item_Fat_Content",  "Item_Identifier",  "Item_MRP",  "Item_Outlet_Sales",  "Item_Visibility",              "Item_Weight",  "Outlet_Establishment_Year",  "Outlet_Identifier",  "Outlet_Location_Type",              "Outlet_Size",  "Outlet_Type"]
        )
        ],
        *transformed_columns
    )

    return df
