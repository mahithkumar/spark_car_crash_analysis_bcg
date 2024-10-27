import yaml
import logging
from typing import Dict
from pyspark.sql import SparkSession, DataFrame

def read_yaml(file_path: str) -> Dict:
    """
    Reads a YAML configuration file and returns it as a dictionary.
    :param file_path: Path to the YAML config file.
    :return: Dictionary with configuration details.
    :raises FileNotFoundError: If the YAML file cannot be found.
    :raises yaml.YAMLError: If the YAML file cannot be parsed.
    """
    try:
        with open(file_path, "r") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logging.error(f"YAML file not found: {file_path}")
        raise  # Re-raise the FileNotFoundError
    except yaml.YAMLError as e:
        logging.error(f"Error parsing YAML file: {e}")
        raise  # Re-raise the YAML error  
    
def read_data_from_csv(
    spark: SparkSession, file_path: str, infer_schema: bool = True, header: bool = True
) -> DataFrame:
    """
    Loads data from a CSV file into a Spark DataFrame.
    :param spark: Spark session instance.
    :param file_path: Path to the CSV file.
    :param infer_schema: Whether to infer schema from the CSV file (default: True).
    :param header: Whether the CSV has a header row (default: True).
    :return: DataFrame containing the loaded data.
    """
    try:
        return spark.read.option("inferSchema", str(infer_schema).lower()).csv(file_path, header=header)
    except Exception as e:
        logging.error(f"Error reading CSV file at {file_path}: {e}")
        raise

def write_data_to_file(df: DataFrame, file_path: str, write_format: str = "csv") -> None:
    """
    Writes a DataFrame to the specified file in the specified format.
    :param df: DataFrame to write.
    :param file_path: Output file path.
    :param write_format: Format to write the file (default: 'csv').
    """
    try:
        df.write.format(write_format).mode("overwrite").option("header", "true").save(file_path)
        # logging.info(f"Data successfully written to {file_path} in {write_format} format.")
    except Exception as e:
        logging.error(f"Error writing file to {file_path}: {e}")
        raise