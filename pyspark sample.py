from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, lit
from pyspark.sql.types import StructType, StructField, StringType
import json

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Dynamic JSON Parsing with Additional Fields") \
    .enableHiveSupport() \
    .getOrCreate()

# Load the configuration JSON file
with open("config.json", "r") as config_file:
    config = json.load(config_file)

# Read the Hive table into a DataFrame
hive_table_df = spark.sql("""
    SELECT 
        workflow_id, 
        activity, 
        activity_status, 
        activity_completion_time, 
        activity_parent, 
        activity_out 
    FROM your_hive_table
""")

# Function to dynamically generate schema for an activity
def generate_schema(fields):
    return StructType([StructField(field, StringType(), True) for field in fields])

# Parse JSON strings and extract fields dynamically
activity_fields = config["activity_fields"]
extracted_dfs = []

for activity, fields in activity_fields.items():
    # Filter rows for the current activity
    activity_df = hive_table_df.filter(col("activity") == activity)
    
    # Generate schema for the current activity
    schema = generate_schema(fields)
    
    # Parse the JSON string and extract fields
    parsed_df = activity_df.withColumn("parsed_json", from_json(col("activity_out"), schema))
    
    # Extract fields dynamically, returning null if the field is not in the JSON
    extracted_df = parsed_df.select(
        "workflow_id",
        col("activity_status").alias(f"{activity}_status"),  # Rename activity_status dynamically
        col("activity_completion_time").alias(f"{activity}_completion_time"),  # Rename completion time
        col("activity_parent").alias(f"{activity}_parent"),  # Rename activity_parent
        *[col(f"parsed_json.{field}").alias(f"{activity}_{field}") for field in fields]
    )
    
    # Append to the list of DataFrames
    extracted_dfs.append(extracted_df)

# Union all DataFrames
combined_df = extracted_dfs[0]
for df in extracted_dfs[1:]:
    combined_df = combined_df.union(df)

# Pivot the data to create a flat view
pivot_df = combined_df.groupBy("workflow_id").pivot("activity").agg(
    *[expr(f"first({activity}_status)").alias(f"{activity}_status") for activity in activity_fields.keys()],
    *[expr(f"first({activity}_completion_time)").alias(f"{activity}_completion_time") for activity in activity_fields.keys()],
    *[expr(f"first({activity}_parent)").alias(f"{activity}_parent") for activity in activity_fields.keys()],
    *[expr(f"first({activity}_{field})").alias(f"{activity}_{field}") 
      for activity, fields in activity_fields.items() 
      for field in fields]
)

# Handle missing fields by adding null columns for fields not in the config
all_activities = activity_fields.keys()
all_fields = set()
for fields in activity_fields.values():
    all_fields.update(fields)

# Add null columns for missing fields in the pivot_df
for activity in all_activities:
    for field in all_fields:
        if f"{activity}_{field}" not in pivot_df.columns:
            pivot_df = pivot_df.withColumn(f"{activity}_{field}", lit(None).cast(StringType()))

# Show the flattened DataFrame
pivot_df.show()

# Optionally, write the result back to Hive or save it to another storage system
pivot_df.write.mode("overwrite").saveAsTable("flattened_workflow_activities")

# Stop the Spark session
spark.stop()