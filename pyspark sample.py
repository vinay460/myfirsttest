from pyspark.sql import SparkSession
from pyspark.sql.functions import col, schema_of_json
import json

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("GenerateConfigJSON") \
    .enableHiveSupport() \  # Enable Hive support
    .getOrCreate()

# Step 1: Read the main data from the Hive table
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

# Step 2: Read distinct activities from the Hive table
activities_df = spark.sql("SELECT DISTINCT activity FROM your_hive_table")
unique_activities = [row["activity"] for row in activities_df.collect()]

# Step 3: Function to extract fields from JSON for a given activity
def extract_json_fields(activity):
    # Filter rows for the current activity
    activity_df = hive_table_df.filter(col("activity") == activity)
    
    # Infer schema from the first non-null JSON string
    sample_json = activity_df.select("activity_out").filter(col("activity_out").isNotNull()).first()[0]
    if sample_json:
        schema = schema_of_json(sample_json)
        return schema.jsonValue()["fields"]
    else:
        return []

# Step 4: Generate activity_fields dictionary
activity_fields = {}
for activity in unique_activities:
    fields = extract_json_fields(activity)
    field_names = [field["name"] for field in fields]
    activity_fields[activity] = field_names

# Step 5: Create the config dictionary
config = {"activity_fields": activity_fields}

# Step 6: Write to config.json
with open("config.json", "w") as config_file:
    json.dump(config, config_file, indent=4)

# Stop the Spark session
spark.stop()



******************


from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, coalesce, from_json, col, expr, lit
from pyspark.sql.types import StructType, StructField, StringType
import json

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("StandardizeColumnAndDynamicJSONParsing") \
    .enableHiveSupport() \  # Enable Hive support
    .getOrCreate()

# Load the configuration JSON file for dynamic JSON parsing
with open("config.json", "r") as config_file:
    config = json.load(config_file)

# Read the config data for standardization from the Hive table
config_df = spark.sql("SELECT input_value, standardized_value FROM config_table")

# Read the main data from the Hive table
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

# Standardize the 'activity' column using the config DataFrame
standardized_df = hive_table_df.join(
    config_df,
    lower(hive_table_df["activity"]) == lower(config_df["input_value"]),
    "left"
).select(
    coalesce(config_df["standardized_value"], hive_table_df["activity"]).alias("activity"),
    hive_table_df["workflow_id"],
    hive_table_df["activity_status"],
    hive_table_df["activity_completion_time"],
    hive_table_df["activity_parent"],
    hive_table_df["activity_out"]
)

# Function to dynamically generate schema for an activity
def generate_schema(fields):
    return StructType([StructField(field, StringType(), True) for field in fields])

# Parse JSON strings and extract fields dynamically
activity_fields = config["activity_fields"]
extracted_dfs = []

for activity, fields in activity_fields.items():
    # Filter rows for the current activity
    activity_df = standardized_df.filter(col("activity") == activity)
    
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