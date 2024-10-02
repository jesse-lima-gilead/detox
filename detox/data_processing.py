import time
import random


def create_data_table(spark):
    """
    Generate a DataFrame containing randomly generated data.

    The DataFrame will have the following columns:
    - 'Scores': Random integers between 0 and 10.
    - 'Temperature': Random integers between -10 and 50.
    - 'RandomValues': Random floating-point numbers between 0 and 1, with some values randomly set to None (NaN).
    - 'NormallyDistributed': Random values generated from a normal distribution with a mean of 10 and a standard deviation of 4.

    Returns:
    DataFrame: A PySpark DataFrame containing the generated data with four columns.
    """
    num_rows = random.choice([95, 200, 300])
    scores = [random.randint(0, 10) for _ in range(num_rows)]
    temperature = [random.randint(-10, 50) for _ in range(num_rows)]
    random_values = [random.uniform(0, 1) for _ in range(num_rows)]
    normally_distributed = [random.gauss(0, 1) for _ in range(num_rows)]

    # Create a DataFrame
    data = {
        'Scores': scores,
        'Temperature': temperature,
        'RandomValues': random_values,
        'NormallyDistributed': normally_distributed
    }
    
    # Convert to DataFrame
    df = spark.createDataFrame([(s, t, r, n) for s, t, r, n in zip(scores, temperature, random_values, normally_distributed)],
                                schema=['Scores', 'Temperature', 'RandomValues', 'NormallyDistributed'])
    return df


def bump_df(df, spark):
    """
    Randomly modifies a Spark DataFrame by altering values in specific columns.

    This function performs the following operations on the input DataFrame `df`:
    
    1. Randomly replaces values in the 'Scores' column with random values between -10 
       and 10. This replacement occurs with a probability of 50%.
       
    2. Randomly inserts NaN (None) values into the 'RandomValues' column with a 
       probability of 30%.
       
    3. Randomly replaces values in the 'Temperature' column with -999 with a 
       probability of 40%.

    Parameters:
    df (pyspark.sql.DataFrame): The input Spark DataFrame to be modified.
    spark (pyspark.sql.SparkSession): The Spark session object (not used in the current implementation).

    Returns:
    pyspark.sql.DataFrame: The modified Spark DataFrame with potential changes in the 
                           'Scores', 'RandomValues', and 'Temperature' columns.
    """

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, when, rand, expr
    import numpy as np

    # Insert random values greater or lower than 10 into random positions in the 'Scores' column
    df = df.withColumn(
        'Scores',
        when(rand() < 0.10, (rand() * 20) - 10).otherwise(col('Scores'))
    )

    # Insert NaNs into random positions in the 'RandomValues' column
    df = df.withColumn(
        'RandomValues',
        when(rand() < 0.10, None).otherwise(col('RandomValues'))
    )
    
    df = df.withColumn(
        'NormallyDistributed',
        when(rand() < 0.10, (rand()*10)).otherwise(col('NormallyDistributed'))
    )

    return df

