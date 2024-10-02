# PySpark Data Ingestion and Transformation Pipeline
This project demonstrates a data pipeline built using PySpark, designed to ingest data from CSV and Parquet files, perform a series of transformations, and load the processed data into a MySQL database. The pipeline is designed to handle various data preprocessing tasks with custom logging for better traceability.

Features
Data Ingestion:

Supports reading data from CSV and Parquet file formats.

Data Transformation:

Adds new columns based on predefined logic.
Converts column data types to ensure consistency.
Concatenates multiple columns into a single one.
Drops unnecessary or redundant columns.
Handles missing values by filling, dropping, or other customizable methods.

Data Loading:

Loads the transformed data into a MySQL database.

Custom Logging:

Implements custom loggers to track each stage of the data pipeline (ingestion, transformation, and loading).
Logs key metrics and transformation steps for easy debugging and monitoring.
Tech Stack
PySpark for data manipulation and transformation.
MySQL for storing the processed data.
Python Logging for custom log tracking.
The project includes custom logging functionality to capture:

Data ingestion status.
Transformation details (added columns, type conversions, null handling).
Errors or exceptions encountered during the process.
Successful data loading operations.
