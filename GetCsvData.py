import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from pyspark.sql.functions import regexp_replace, col, when


# Function 1: Download CSV file from URL
def download_csv(url, output_file):
    response = requests.get(url)
    with open(output_file, 'wb') as file:
        file.write(response.content)
    print(f"CSV downloaded: {output_file}")


# Function 2: Load CSV into Pandas DataFrame
def load_to_pandas(file_path):
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(file_path, encoding='ISO-8859-1')
    print("Loaded data into Pandas DataFrame")
    return df


# Function 3: Convert Pandas DataFrame to Spark DataFrame
def convert_to_spark(pandas_df, spark, schema=None):
    if schema:
        spark_df = spark.createDataFrame(pandas_df, schema=schema)
    else:
        spark_df = spark.createDataFrame(pandas_df)
    print("Converted to Spark DataFrame")
    return spark_df


# Function 4: Data Cleaning and Manipulation
def clean_data(spark_df):
    # Define a dictionary of old column names (from the CSV) and their new names (for the schema)
    column_rename_mapping = {
        "ENCOUNTERCLASS": "encounter_class",
        "REASONCODE": "reason_code",
        "REASONDESCRIPTION": "reason_description"
    }

    # Rename columns using a loop
    for old_col, new_col in column_rename_mapping.items():
        if old_col in spark_df.columns:
            spark_df = spark_df.withColumnRenamed(old_col, new_col)
    #convert headings to lowercase
    spark_df = spark_df.toDF(*[col.lower() for col in spark_df.columns])
    spark_df = spark_df.fillna('')

    # Additionally, handle 'NaN' as a string and replace it with empty string as well
    spark_df = spark_df.replace('NaN', '')

    # Convert `start` and `stop` to timestamp format
#     spark_df = spark_df.withColumn('start', to_timestamp(col('start'), 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\''))
#     spark_df = spark_df.withColumn('stop', to_timestamp(col('stop'), 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\''))

    numeric_columns = ['base_encounter_cost', 'total_claim_cost', 'payer_coverage']
    for col_name in numeric_columns:
        # Replace non-numeric characters except dots
        spark_df = spark_df.withColumn(col_name, regexp_replace(col(col_name), '[^0-9.]', ''))
        # Cast to FloatType
        spark_df = spark_df.withColumn(col_name, col(col_name).cast(FloatType()))
#         spark_df = spark_df.withColumn(col_name, col(col_name).cast(StringType()))

    # Assuming you want to prepend '$' sign to `data_value` column
    currency_columns = ['base_encounter_cost', 'total_claim_cost', 'payer_coverage']
    for col_name in currency_columns:
        if col_name in spark_df.columns:
            spark_df = spark_df.withColumn(
                col_name,
                when(~col(col_name).startswith('$'),  # Check if the value doesn't start with $
                     regexp_replace(col(col_name), '^', '\\$'))  # Prepend $ to those values
                .otherwise(col(col_name))  # Leave values with $ unchanged
            )
    print("Data cleaned")
    return spark_df


# Function 5: Define the Spark Schema
def define_schema():
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("start", StringType(), True),
        StructField("stop", StringType(), True),
        StructField("patient", StringType(), True),
        StructField("organization", StringType(), True),
        StructField("payer", StringType(), True),
        StructField("encounter_class", StringType(), True),
        StructField("code", StringType(), True),
        StructField("description", StringType(), True),
        StructField("base_encounter_cost", FloatType(), True),
        StructField("total_claim_cost", FloatType(), True),
        StructField("payer_coverage", FloatType(), True),
        StructField("reason_code", StringType(), True),
        StructField("reason_description", StringType(), True),
    ])
    return schema


# Function 6: Load the cleaned Spark DataFrame into PostgreSQL
def load_to_postgres(spark_df):
    jdbc_url = "jdbc:postgresql://localhost:5432/hospital_db"  # 'host.docker.internal' resolves to localhost in Docker
    jdbc_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }

    # Write the DataFrame to the PostgreSQL table
    spark_df.write.jdbc(url=jdbc_url, table="cleaned_data", mode="overwrite", properties=jdbc_properties)
    print("Data loaded into PostgreSQL")


# Function 7: Main execution logic
def main():
    csv_url = "https://drive.google.com/uc?export=download&id=1F3ck4BTILa8bAU1GIr7x76TANuTsD-1E"
    output_file = "dataset.csv"

    # Step 1: Download CSV
    download_csv(csv_url, output_file)

    # Step 2: Load CSV into Pandas DataFrame
    pandas_df = load_to_pandas(output_file)

    # Step 3: Initialize Spark
    spark = SparkSession.builder \
        .appName("CSV to Spark") \
        .config("spark.jars", "./postgresql-42.7.3.jar") \
        .getOrCreate()

    # Step 4: Define schema for Spark DataFrame
    schema = define_schema()

    # Step 5: Convert Pandas DataFrame to Spark DataFrame with schema
    spark_df = convert_to_spark(pandas_df, spark, schema=schema)

    # Step 6: Perform data cleaning and manipulation
    cleaned_spark_df = clean_data(spark_df)

    # Step 7: Load the cleaned data into PostgreSQL
    load_to_postgres(cleaned_spark_df)
    # load_to_postgres(spark_df)


if __name__ == "__main__":
    main()
