import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import struct, to_json


def spark_df(pandas_df: pd.DataFrame, 
             spark: SparkSession) -> pyspark.DataFrame : 
    
    df_spark = spark.createDataFrame(pandas_df)
    schema = df_spark.schema
    
    df_spark.printSchema()
    

    df_spark = df_spark.withColumn("json", to_json(struct("EBITDA", "EBIT"))) \
        .select("json")
    
    return df_spark

