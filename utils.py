"""
In this file all utility functions for the project are stored. 
All functions are documented in the code.

Functions:
    calculate_seconds:  Convert nanoseconds to seconds.
    create_random_df:   Create a DataFrame with random missing values.
    iterations:         Execute a test function multiple times and calculate the average time taken for both Pandas and PySpark.
    plot_statistic:     Plot a statistic for both Pandas and PySpark results.
    test_run:           Iterate over a list of dataframes to execute a test function.
"""

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pyspark

# garbage collector
import gc

from statistics import mean


def calculate_seconds(nanoseconds: int):
    """
    Convert the given time in nanoseconds to seconds.

    Args:
        nanoseconds (int): Time in nanoseconds.

    Returns:
        float: Time converted to seconds.
    """

    return nanoseconds / 1e9


def create_random_df(row_count: int, column_count: int, missing_prob=0.2):
    """
    Create a DataFrame with random missing values.

    Args:
        row_count (int): The number of rows in the DataFrame.
        column_count (int): The number of columns in the DataFrame.
        missing_prob (float, optional): The probability of a value being missing. Defaults to 0.2.

    Returns:
        pandas.DataFrame: A randomly generated DataFrame with missing values.

    """
    
    columns = [f"col_{i}" for i in range(column_count)]
    data = np.random.randn(row_count, column_count)
    df = pd.DataFrame(data, columns=columns)

    # Introduce missing values randomly
    mask = np.random.random(df.shape) < missing_prob
    df[mask] = np.nan

    return df


def test_iterations(test_func: object, iterations: int, spark_session, df_pd: pd.DataFrame, df_spark: pyspark.sql.DataFrame):
    """
    Execute a test function multiple times and calculate the average time taken for both pandas and PySpark.

    Args:
        test_func(object): The function to be executed multiple times. It should return a tuple of (pandas_time, pyspark_time).
        iterations (int): The number of iterations to perform.
        spark_session (SparkSession): The SparkSession object.
        df_pd (pd.DataFrame): The Pandas DataFrame to be used in the test function.
        df_spark (pyspark.sql.Dataframe): The Spark DataFrame to be used in the test function.

    Returns:
        tuple: A tuple containing the average time taken for pandas (float) and PySpark (float), along with two lists of individual execution times.
    """

    # disable garbage collector to avoid side effects
    gc.disable()

    pandas_times = []
    pyspark_times = []

    for _ in range(iterations):
        # execute test function
        pandas_time, pyspark_time = test_func(df_pd, df_spark, spark_session)
        pandas_times.append(pandas_time)
        pyspark_times.append(pyspark_time)

    # re-enable garbage collector
    gc.enable()

    average_pandas_time = mean(pandas_times)
    average_pyspark_time = mean(pyspark_times)
    return average_pandas_time, average_pyspark_time, pandas_times, pyspark_times


def plot_statistic(title: str, df: pd.DataFrame, column_name: str):
    """
    Generate a line plot to compare the execution time of a statistic between Pandas and PySpark.

    Args:
        title (str): Title of the plot.
        df (pd.DataFrame): DataFrame containing the statistics and row counts for Pandas and PySpark.
        column_name (str): Name of the statistic column to plot.

    Returns:
        plotly.graph_objects.Figure: The generated line plot.
    """
    
    # create figure
    fig = go.Figure()

    # add the traces
    fig.add_trace(go.Scatter(x=df["row_count"], y=df[f"{column_name}_pd"].apply(calculate_seconds), mode='lines', name='Pandas', hovertemplate='Rows: %{x}<br>Seconds: %{y}'))
    fig.add_trace(go.Scatter(x=df["row_count"], y=df[f"{column_name}_spark"].apply(calculate_seconds), mode='lines', name='PySpark',hovertemplate='Rows: %{x}<br>Seconds: %{y}'))

    # update the layout
    fig.update_layout(title=title + " - Time Comparison",
                    xaxis_title="Number of Rows",
                    yaxis_title="Seconds")
                    
    return fig


def test_run(test_func: callable, pandas_column_name: str, spark_column_name: str, statistics_df: pd.DataFrame, dataframes_pd: list, dataframes_spark: list, spark_session: pyspark.sql.SparkSession, iterations=5):
    """
    Run performance tests for a specific function on Pandas and PySpark data frames.

    Parameters:
        function (callable): The function to be tested.
        pandas_column_name (str): Name of the column to store the Pandas execution time.
        spark_column_name (str): Name of the column to store the PySpark execution time.
        statistics_df (pandas.DataFrame): DataFrame to store the statistics of the performance tests.
        dataframes_pd (List[pandas.DataFrame]): List of Pandas data frames to be tested.
        dataframes_spark (List[pyspark.sql.DataFrame]): List of PySpark data frames to be tested.
        spark_session (SparkSession): The SparkSession object.
        iterations (int, optional): Number of iterations to run for each data frame. Default is 5.

    Returns:
        pandas.DataFrame: The updated statistics DataFrame.
    """

    # iterate over dataframes
    for i in range(len(dataframes_pd)):
        df_pd = dataframes_pd[i]
        df_spark = dataframes_spark[i]
        print(df_pd.shape)
        average_pandas_time, average_pyspark_time, _ , _ = test_iterations(test_func, iterations, spark_session, df_pd, df_spark)
        statistics_df.loc[(statistics_df['row_count'] == df_pd.shape[0]) & (statistics_df["column_count"] == df_pd.shape[1]), [pandas_column_name, spark_column_name]] = [average_pandas_time, average_pyspark_time]
    
    return statistics_df
