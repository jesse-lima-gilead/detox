from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
from pyspark.sql import functions as F
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult
from pyspark.sql import SparkSession

import pydeequ
import json
import yaml


class PyDeequDynamicParser:
    """
    A dynamic parser for applying data quality checks using PyDeequ.

    This class takes a PyDeequ Check instance and a set of user-defined 
    checks, and dynamically applies the specified checks to the instance 
    using the appropriate methods.

    Attributes:
    - pydeequ_check_instance: Check
        An instance of the PyDeequ Check class to which the checks will be applied.
    
    - user_checks: dict
        A dictionary containing user-defined checks and their parameters.
    
    - functions_map: dict
        A mapping of check names to their corresponding methods in the class.

    Parameters:
    - pydeequ_check_instance: Check
        An instance of the PyDeequ Check class.
    
    - user_checks: dict
        A dictionary containing user-defined checks and their parameters.
    """
    def __init__(self, pydeequ_check_instance, user_checks):
        self.pydeequ_check_instance = pydeequ_check_instance
        self.user_checks = user_checks
        self.functions_map = {
            "isComplete": self.dq_is_complete,
            "isUnique": self.dq_is_unique,
            "hasMin": self.dq_has_min,
            "hasMax": self.dq_has_max,
            "hasSize": self.dq_has_size,
            "isNonNegative": self.dq_is_non_negative,
            "hasMean": self.dq_has_mean,
            "hasStandardDeviation": self.dq_has_standard_deviation,
        }

    def dq_is_complete(self, column, hint):
        self.pydeequ_check_instance = self.pydeequ_check_instance.isComplete(column, hint)

    def dq_is_unique(self, column, hint):
        self.pydeequ_check_instance = self.pydeequ_check_instance.isUnique(column, hint)

    def dq_has_min(self, column, value, hint):
        self.pydeequ_check_instance = self.pydeequ_check_instance.hasMin(column, lambda x: x >= value)

    def dq_has_max(self, column, value, hint):
        self.pydeequ_check_instance = self.pydeequ_check_instance.hasMax(column, lambda x: x <= value)

    def dq_has_size(self, value, hint):
        self.pydeequ_check_instance = self.pydeequ_check_instance.hasSize(lambda x: x == value)

    def dq_is_non_negative(self, column, hint):
        self.pydeequ_check_instance = self.pydeequ_check_instance.isNonNegative(column)

    def dq_has_mean(self, column, value, hint):
        self.pydeequ_check_instance = self.pydeequ_check_instance.hasMean(column, lambda x: x == value)

    def dq_has_standard_deviation(self, column, value, hint):
        self.pydeequ_check_instance = self.pydeequ_check_instance.hasStandardDeviation(column, lambda x: x == value)
        

    def parse(self):
        """
        Parse and apply user-defined checks to the PyDeequ Check instance.

        This method iterates through the user-defined checks and applies 
        each check to the PyDeequ Check instance using the corresponding 
        method from the functions_map.

        Returns:
        - Check
            The updated PyDeequ Check instance after applying all checks.
        """
        for current_check in self.user_checks['checks']:
            current_name = current_check['name']
            current_parameters = current_check['parameters']

            #print(f"Calling {current_name} with parameters: {current_parameters}")  # Debugging line

            if current_name in self.functions_map:
                func = self.functions_map[current_name]
                func(**current_parameters)
            else:
                raise ValueError(f"Function {current_name} is not defined in functions_map.")

        return self.pydeequ_check_instance


def pydeequ_check(df, spark, check_file = './data_quality_checks.yaml'):
    """
    Perform data quality checks on a Spark DataFrame using PyDeequ.

    This function loads data quality checks from a specified YAML file, 
    creates a Check instance, and applies the checks to the provided 
    Spark DataFrame. It then runs a verification suite and returns the 
    results in JSON format.

    Parameters:
    - df: pyspark.sql.DataFrame
        The Spark DataFrame on which to perform data quality checks.
    
    - spark: pyspark.sql.SparkSession
        The Spark session used to run the checks.
    
    - check_file: str, optional
        The path to the YAML file containing the data quality checks. 
        Defaults to './data_quality_checks.yaml'.

    Returns:
    - dict
        A dictionary containing the results of the data quality checks 
        in JSON format.
    """
    
    # Load checks from YAML file
    with open(check_file, 'r') as file:
        user_checks = yaml.safe_load(file)

    # Create a Check instance
    check = Check(spark, CheckLevel.Warning, "Data Quality Checks")

    # Create a dynamic parser instance
    parser = PyDeequDynamicParser(check, user_checks)

    # Parse and apply checks
    final_check = parser.parse()

    # Create a verification suite
    verification_suite = VerificationSuite(spark) \
        .onData(df) \
        .addCheck(final_check)
    
    verification_result = verification_suite.run()

    # Show the results
    return VerificationResult.checkResultsAsJson(spark, verification_result)