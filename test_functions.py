""" 
This file contains the test functions for the Pandas and PySpark comparison. 


Each function has the same structure:

Args:
    df_pd (pandas.DataFrame): The Pandas DataFrame to test.
    df_spark (pyspark.sql.DataFrame): The PySpark DataFrame to test.
    spark_session (SparkSession): The SparkSession object.

Returns:
    pandas_time (int): The time taken for the Pandas DataFrame.
    pyspark_time (int): The time taken for the PySpark DataFrame.

Please find a description of the test functions in the code as docstring below.

Note that sometime a parameter is initialized but not used (initialized as _). 
This is because I wanted to have a general structure to run the code and I didn't want to change the code for each test function.


Functions (ordered by test order):
    write_data:             Write a DataFrame to a csv file.
    load_data:              Load a DataFrame from a csv file.
    drop_nan:               Drop all rows with missing values.
    fill_nan:               Fill all missing values with 0.
    group_df:               Group the DataFrame by a column.
    group_sum_df:           Group the DataFrame by a column and sum the values.
    group_count_df:         Group the DataFrame by a column and count the values.
    filter_less_0:          Filter the DataFrame by a column value less than 0.
    filter_less_10:         Filter the DataFrame by a column value less than 10.
    join_df:                Join two DataFrames.
    multiply_build_in:      Multiply one column of the DataFrame with a constant (using the build in function).
    multiply_by_selection:  Multiply one column of the DataFrame with a constant (using column selection).
    convert_df:             Convert the DataFrame to a different format (Pandas -> PySpark and vice versa).
"""

import time
import pandas as pd
import pyspark.sql.functions as F


def write_data(df_pd, df_spark, _):
    ### start pandas timer ###
    pd_counter_start = time.perf_counter_ns()
    # Pandas test code
    df_pd.to_csv(f"test/pandas_test.csv")
    ### end pandas timer ###
    pd_counter_end = time.perf_counter_ns()

    ### start spark timer
    spark_counter_start = time.perf_counter_ns()
    # PySpark test code 
    df_spark.write.mode("overwrite").save("test/spark_test_v1.csv")
    # end spark timer
    spark_counter_end = time.perf_counter_ns()

    pandas_time = pd_counter_end - pd_counter_start
    pyspark_time = spark_counter_end - spark_counter_start

    return pandas_time, pyspark_time


def load_data(df_pd, _, spark_session):
    len = df_pd.shape[0]
    path = "small" if len < 100_000 else "large"
    ### start pandas timer ###
    pd_counter_start = time.perf_counter_ns()
    # Pandas test code
    df = pd.read_csv(f"data/{path}/pandas_test_{len}_rows.csv")

    ### end pandas timer ###
    pd_counter_end = time.perf_counter_ns()

    ### start spark timer
    spark_counter_start = time.perf_counter_ns()
    # PySpark test code 
    df = spark_session.read.csv(f"data/{path}/pandas_test_{len}_rows.csv", header=True, inferSchema=True, sep=",")

    # end spark timer
    spark_counter_end = time.perf_counter_ns()

    pandas_time = pd_counter_end - pd_counter_start
    pyspark_time = spark_counter_end - spark_counter_start

    return pandas_time, pyspark_time


def drop_nan(df_pd, df_spark, _):
    ### start pandas timer ###
    pd_counter_start = time.perf_counter_ns()
    # Pandas test code
    df_pd = df_pd.dropna()
    
    ### end pandas timer ###
    pd_counter_end = time.perf_counter_ns()

    ### start spark timer
    spark_counter_start = time.perf_counter_ns()
    # PySpark test code 
    df_spark = df_spark.na.drop("all")

    # end spark timer
    spark_counter_end = time.perf_counter_ns()

    pandas_time = pd_counter_end - pd_counter_start
    pyspark_time = spark_counter_end - spark_counter_start

    return pandas_time, pyspark_time


def fill_nan(df_pd, df_spark, _):
    ### start pandas timer ###
    pd_counter_start = time.perf_counter_ns()
    # Pandas test code
    df_pd = df_pd.fillna(0)
    
    ### end pandas timer ###
    pd_counter_end = time.perf_counter_ns()

    ### start spark timer
    spark_counter_start = time.perf_counter_ns()
    # PySpark test code 
    df_spark = df_spark.na.fill(0)

    # end spark timer
    spark_counter_end = time.perf_counter_ns()

    pandas_time = pd_counter_end - pd_counter_start
    pyspark_time = spark_counter_end - spark_counter_start

    return pandas_time, pyspark_time


def group_df(df_pd, df_spark, _):
    ### start pandas timer ###
    pd_counter_start = time.perf_counter_ns()
    # Pandas test code
    pd_grouped = df_pd.groupby("col_0")
    
    ### end pandas timer ###
    pd_counter_end = time.perf_counter_ns()

    ### start spark timer
    spark_counter_start = time.perf_counter_ns()
    # PySpark test code 
    spark_grouped = df_spark.groupby("col_0")

    # end spark timer
    spark_counter_end = time.perf_counter_ns()

    pandas_time = pd_counter_end - pd_counter_start
    pyspark_time = spark_counter_end - spark_counter_start

    return pandas_time, pyspark_time


def group_sum_df(df_pd, df_spark, _):
    ### start pandas timer ###
    pd_counter_start = time.perf_counter_ns()
    # Pandas test code
    pd_grouped = df_pd.groupby("col_0").sum()
    
    ### end pandas timer ###
    pd_counter_end = time.perf_counter_ns()

    ### start spark timer
    spark_counter_start = time.perf_counter_ns()
    # PySpark test code 
    spark_grouped = df_spark.groupby("col_0").sum()

    # end spark timer
    spark_counter_end = time.perf_counter_ns()

    pandas_time = pd_counter_end - pd_counter_start
    pyspark_time = spark_counter_end - spark_counter_start

    return pandas_time, pyspark_time


def group_count_df(df_pd, df_spark, _):
    ### start pandas timer ###
    pd_counter_start = time.perf_counter_ns()
    # Pandas test code
    pd_grouped = df_pd.groupby("col_0").count()
    
    ### end pandas timer ###
    pd_counter_end = time.perf_counter_ns()

    ### start spark timer
    spark_counter_start = time.perf_counter_ns()
    # PySpark test code 
    spark_grouped = df_spark.groupby("col_0").count()

    # end spark timer
    spark_counter_end = time.perf_counter_ns()

    pandas_time = pd_counter_end - pd_counter_start
    pyspark_time = spark_counter_end - spark_counter_start

    return pandas_time, pyspark_time


def filter_less_0(df_pd, df_spark, _):
    ### start pandas timer ###
    pd_counter_start = time.perf_counter_ns()
    # Pandas test code
    filtered_df = df_pd[df_pd.col_0 > 0]
    
    ### end pandas timer ###
    pd_counter_end = time.perf_counter_ns()

    ### start spark timer
    spark_counter_start = time.perf_counter_ns()
    # PySpark test code 
    filtered_df = df_spark.filter(df_spark.col_0 > 0)

    # end spark timer
    spark_counter_end = time.perf_counter_ns()

    pandas_time = pd_counter_end - pd_counter_start
    pyspark_time = spark_counter_end - spark_counter_start

    return pandas_time, pyspark_time


def filter_less_10(df_pd, df_spark, _):
    ### start pandas timer ###
    pd_counter_start = time.perf_counter_ns()
    # Pandas test code
    filtered_df = df_pd[df_pd.col_0 > 10]
    
    ### end pandas timer ###
    pd_counter_end = time.perf_counter_ns()

    ### start spark timer
    spark_counter_start = time.perf_counter_ns()
    # PySpark test code 
    filtered_df = df_spark.filter(df_spark.col_0 > 10)

    # end spark timer
    spark_counter_end = time.perf_counter_ns()

    pandas_time = pd_counter_end - pd_counter_start
    pyspark_time = spark_counter_end - spark_counter_start

    return pandas_time, pyspark_time


def join_df(df_pd, df_spark, _):
    ### start pandas timer ###
    pd_counter_start = time.perf_counter_ns()
    # Pandas test code
    merged_df = pd.merge(df_pd, df_pd, on="col_0")

    ### end pandas timer ###
    pd_counter_end = time.perf_counter_ns()

    ### start spark timer
    spark_counter_start = time.perf_counter_ns()
    # PySpark test code 
    joined_df = df_spark.join(df_spark, on="col_0", how="inner")

    # end spark timer
    spark_counter_end = time.perf_counter_ns()

    pandas_time = pd_counter_end - pd_counter_start
    pyspark_time = spark_counter_end - spark_counter_start

    return pandas_time, pyspark_time


def multiply_build_in(df_pd, df_spark, _):
    ### start pandas timer ###
    pd_counter_start = time.perf_counter_ns()
    # Pandas test code
    df_pd["col_0"] = df_pd["col_0"].mul(2)

    ### end pandas timer ###
    pd_counter_end = time.perf_counter_ns()

    ### start spark timer
    spark_counter_start = time.perf_counter_ns()
    # PySpark test code 
    df_spark = df_spark.withColumn("col_0", F.expr("col_0 * 2"))

    # end spark timer
    spark_counter_end = time.perf_counter_ns()

    pandas_time = pd_counter_end - pd_counter_start
    pyspark_time = spark_counter_end - spark_counter_start

    return pandas_time, pyspark_time


def multiply_by_selection(df_pd, df_spark, _):
    ### start pandas timer ###
    pd_counter_start = time.perf_counter_ns()
    # Pandas test code
    df_pd["col_0"] = df_pd["col_0"] * 2

    ### end pandas timer ###
    pd_counter_end = time.perf_counter_ns()

    ### start spark timer
    spark_counter_start = time.perf_counter_ns()
    # PySpark test code 
    df_spark = df_spark.withColumn("col_0", F.col("col_0") * 2)

    # end spark timer
    spark_counter_end = time.perf_counter_ns()

    pandas_time = pd_counter_end - pd_counter_start
    pyspark_time = spark_counter_end - spark_counter_start

    return pandas_time, pyspark_time

def convert_df(df_pd, df_spark, spark_session):
    ### start pandas timer ###
    pd_counter_start = time.perf_counter_ns()
    # Pandas to PySpark
    df = spark_session.createDataFrame(df_pd)

    ### end pandas timer ###
    pd_counter_end = time.perf_counter_ns()

    ### start spark timer
    spark_counter_start = time.perf_counter_ns()
    # PySpark to Pandas
    df = df_spark.toPandas()

    # end spark timer
    spark_counter_end = time.perf_counter_ns()

    pandas_time = pd_counter_end - pd_counter_start
    pyspark_time = spark_counter_end - spark_counter_start

    return pandas_time, pyspark_time
