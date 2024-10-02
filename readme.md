
# Detox Project - Gilead Hackathon 2023

## Overview

The Detox project is designed to run simulations for data quality checks using PyDeequ/Cloudwatch. Users can execute the simulation either from a Jupyter Notebook or by running the `main.py` script. In both cases, a PySpark environment must be available.

### Running the Simulation

1. **From Jupyter Notebook**: 
   - You can run the simulation directly in the provided Jupyter Notebooks. The necessary environment variables, including `SPARK_VERSION`, are already set up in the notebook.

2. **From `main.py`**:
   - If you choose to run the simulation using the `main.py` script, you must export the `SPARK_VERSION` environment variable in your terminal before executing the script. 
   - To do this, run the following command in your terminal:
     ```bash
     export SPARK_VERSION=3.3  # Change this to your Spark version if needed
     ```
   - Then, execute the script with:
     ```bash
     python -m your_package.main
     ```

### Main Functions

The main functions in this project are set up to run 1000 random examples, simulating various scenarios to validate data quality.

## Project Structure

The project is organized as follows:

```
your_project/
├── your_package/
│   ├── __init__.py
│   ├── data_processing.py      # Helper code to generate randomization for the proof of concept
│   ├── data_quality.py         # Code related to PyDeequ data quality checks and helpers
│   ├── aws_logging.py          # Code related to writing logs into AWS CloudWatch
│   ├── pydeequ_dynamic_parser.py # Dynamic parser for PyDeequ checks
├── main.py                     # Entry point for running the simulation
├── notebooks/                  # Contains Jupyter Notebooks for simulation
├── data_quality_checks.yaml     # YAML file with rules for the dynamic parser
├── requirements.txt            # Required packages for a successful run
└── README.md                   # Project documentation
```

### Configuration

To run the data quality checks, the user should maintain a YAML file named `data_quality_checks.yaml` in the main folder. This file should contain the rules in a standard format that can be read by the dynamic parser.

### Requirements

The `requirements.txt` file includes all the necessary packages required for a successful run of the project. Make sure to install these packages in your Python environment:

```bash
pip install -r requirements.txt
```

## Conclusion

This project provides a robust framework for simulating data quality checks using PySpark and PyDeequ. Whether you choose to run the simulation from a Jupyter Notebook or via the command line, ensure that your environment is properly configured to achieve the best results.
