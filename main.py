import os
import pydeequ
import pyspark.sql
from pyspark.sql import SparkSession

import time
import random
import boto3

# Main function. Make sure you have a credential file accessible for boto3
# This main function makes 10 trials for pydeequ + cloudwatch
if __name__ == "__main__":

    os.environ["SPARK_VERSION"] = "3.3"  # Change this to your version if needed
    from detox.data_processing import create_data_table, bump_df
    from detox.data_quality import pydeequ_check
    from detox.aws_logging import log_data_quality

    # Initialize Spark session with Deequ package
    spark = (SparkSession
            .builder
            .config("spark.jars.packages", pydeequ.deequ_maven_coord)
            .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
            .getOrCreate())
    
    # Create a session with your access keys
    os.environ['AWS_SHARED_CREDENTIALS_FILE'] = '.aws/credentials'
    #session = boto3.Session(region_name='us-east-1')
    session = boto3.Session(profile_name='hackathon', region_name='us-west-2')

    # Create a CloudWatch Logs client
    client = session.client('logs')

    # Set log group and log stream for our experiment
    log_group = 'hackathon'
    log_stream = 'detox_usecase'
    
    # Create 100 simulations and log results to CloudWatch
    for i in range(1,10):
        df = create_data_table(spark)
        bdf = bump_df(df, spark)
        json_result = pydeequ_check(bdf, spark, check_file = './data_quality_checks.yaml')
        #df_result.show()

        log_data_quality(json_result, client, log_group, log_stream)

        # Waits between 1 and 5 seconds until next simulation
        delay =random.randint(1,5)
        print(f"Simulation {i}, next step = {delay}s")
        time.sleep(delay)
