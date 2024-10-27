# Car Crash Spark Application

## Overview

The Car Crash Spark Application is a data processing application built with Apache Spark that analyzes car crash data to extract valuable insights related to accidents. The application leverages Spark's distributed computing capabilities to efficiently process large datasets, making it suitable for data engineering and analytics tasks.

## Analysis

- Count crashes involving males greater than 2.
- Analyze two-wheeler accidents.
- Determine the top vehicle makes involved in crashes.
- Identify hit-and-run incidents with valid driver licenses.
- Find states with the highest number of accidents without female involvement.
- Analyze vehicle makes contributing to injuries.
- Identify top ethnic user groups for various body styles.
- Analyze crash data involving alcohol-related incidents.
- Identify distinct crash IDs with no damage.
- Determine the top vehicle makes associated with speeding offenses.

## Prerequisites

- Python 3.6 or higher
- Apache Spark 3.0 or higher
- Required Python libraries:
  - PySpark
  - PyYAML

## Project Structure

```plaintext
.
├── configs
│   └── config.yaml            # Configuration file for output paths and file formats
├── input_data
│   └── input_files_csv        # Directory for input CSV files
├── output_files
│   └── output_files           # Directory for output files
├── src
│   ├── main.py                # Main entry point of the application
│   ├── analysis.py            # Module containing analysis functions
│   ├── utils.py               # Utility functions for reading and writing data
│   └── query_outputs.py       # Consists of queries and their outputs
