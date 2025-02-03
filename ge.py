Certainly! If the **column name** is coming as part of the **rule parameters** (e.g., in the `rule_parameters` JSON field), we can adjust the code to extract the column name dynamically from the parameters. This makes the rule engine more flexible and aligns with the requirement.

Below is the **adjusted code** where the column name is extracted from the `rule_parameters` JSON field.

---

## **Adjusted Code**

```python
import pymysql
import json
import great_expectations as ge
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime

# Database connection details
DB_HOST = "localhost"
DB_USER = "user"
DB_PASSWORD = "password"
DB_NAME = "rule_db"

# Fetch rules from the database
def fetch_rules(bri):
    """
    Fetches rules from the database based on the Business Rule Identifier (BRI).
    """
    connection = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, db=DB_NAME)
    cursor = connection.cursor()
    query = "SELECT rule_type, rule_parameters, action FROM data_quality_rules WHERE bri = %s"
    cursor.execute(query, (bri,))
    rules = cursor.fetchall()
    cursor.close()
    connection.close()
    return [{"rule_type": row[0], "rule_parameters": json.loads(row[1]), "action": row[2]} for row in rules]

# Apply rules using Great Expectations
def apply_rules(df, rules):
    """
    Applies Great Expectations rules to the PySpark DataFrame.
    """
    ge_df = ge.dataset.SparkDFDataset(df)
    validation_results = []

    for rule in rules:
        rule_type = rule["rule_type"]
        params = rule["rule_parameters"]
        action = rule["action"]

        try:
            if rule_type == "expect_column_values_to_be_between":
                column_name = params["column"]
                validation = ge_df.expect_column_values_to_be_between(column_name, **params)
            elif rule_type == "expect_column_values_to_match_regex":
                column_name = params["column"]
                validation = ge_df.expect_column_values_to_match_regex(column_name, **params)
            elif rule_type == "expect_column_values_to_not_be_null":
                column_name = params["column"]
                validation = ge_df.expect_column_values_to_not_be_null(column_name)
            elif rule_type == "expect_column_values_to_be_unique":
                column_name = params["column"]
                validation = ge_df.expect_column_values_to_be_unique(column_name)
            elif rule_type == "expect_column_values_to_be_in_set":
                column_name = params["column"]
                validation = ge_df.expect_column_values_to_be_in_set(column_name, **params)
            elif rule_type == "expect_column_values_to_not_be_in_set":
                column_name = params["column"]
                validation = ge_df.expect_column_values_to_not_be_in_set(column_name, **params)
            elif rule_type == "expect_column_values_to_be_of_type":
                column_name = params["column"]
                validation = ge_df.expect_column_values_to_be_of_type(column_name, **params)
            elif rule_type == "expect_column_values_to_match_strftime_format":
                column_name = params["column"]
                validation = ge_df.expect_column_values_to_match_strftime_format(column_name, **params)
            elif rule_type == "expect_column_values_to_be_increasing":
                column_name = params["column"]
                validation = ge_df.expect_column_values_to_be_increasing(column_name)
            elif rule_type == "expect_column_values_to_be_decreasing":
                column_name = params["column"]
                validation = ge_df.expect_column_values_to_be_decreasing(column_name)
            elif rule_type == "expect_column_values_to_be_dateutil_parseable":
                column_name = params["column"]
                validation = ge_df.expect_column_values_to_be_dateutil_parseable(column_name)
            elif rule_type == "expect_column_values_to_be_json_parseable":
                column_name = params["column"]
                validation = ge_df.expect_column_values_to_be_json_parseable(column_name)
            elif rule_type == "expect_column_values_to_match_json_schema":
                column_name = params["column"]
                validation = ge_df.expect_column_values_to_match_json_schema(column_name, **params)
            elif rule_type == "expect_table_row_count_to_be_between":
                validation = ge_df.expect_table_row_count_to_be_between(**params)
            elif rule_type == "expect_column_null_count_to_be_between":
                column_name = params["column"]
                validation = ge_df.expect_column_null_count_to_be_between(column_name, **params)
            elif rule_type == "expect_table_column_count_to_equal":
                validation = ge_df.expect_table_column_count_to_equal(**params)
            elif rule_type == "expect_column_values_to_be_within_last_n_days":
                column_name = params["column"]
                validation = ge_df.expect_column_values_to_be_within_last_n_days(column_name, **params)
            else:
                raise ValueError(f"Unsupported rule type: {rule_type}")

            validation_results.append({
                "rule_type": rule_type,
                "column_name": column_name if "column" in params else None,
                "success": validation.success,
                "result": validation.result,
                "action": action
            })
        except Exception as e:
            print(f"Error applying rule {rule_type}: {e}")
            validation_results.append({
                "rule_type": rule_type,
                "column_name": params.get("column", None),
                "success": False,
                "result": {"exception": str(e)},
                "action": action
            })

    return validation_results

# Handle actions based on validation results
def handle_actions(df, validation_results):
    """
    Handles actions (e.g., log, reject, transform) based on validation results.
    """
    for result in validation_results:
        column_name = result.get("column_name")
        action = result["action"]

        if action == "reject" and column_name:
            # Filter out rows that fail the rule
            if "unexpected_count" in result["result"]:
                unexpected_indices = result["result"]["partial_unexpected_list"]
                df = df.filter(~col(column_name).isin(unexpected_indices))
        elif action == "log":
            # Log the validation result
            print(f"Validation failed for rule {result['rule_type']}: {result['result']}")
        elif action == "transform" and column_name:
            # Apply transformation logic here (e.g., replace invalid values)
            if "unexpected_count" in result["result"]:
                unexpected_indices = result["result"]["partial_unexpected_list"]
                df = df.withColumn(column_name, col(column_name).when(col(column_name).isin(unexpected_indices), "DEFAULT_VALUE"))

    return df

# Main function to execute the rule engine
def data_quality_rule_engine(df, bri):
    """
    Main function to execute the Data Quality Rule Engine.
    """
    # Step 1: Fetch rules
    rules = fetch_rules(bri)

    # Step 2: Apply rules
    validation_results = apply_rules(df, rules)

    # Step 3: Handle actions
    df = handle_actions(df, validation_results)

    return df

# Example usage
if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("DataQualityEngine").getOrCreate()

    # Example DataFrame
    data = [("john@example.com", 25, "2023-10-01"), ("invalid-email", 17, "2023-09-01"), ("jane@example.com", 30, "2023-10-15")]
    columns = ["email", "age", "last_updated"]
    df = spark.createDataFrame(data, columns)

    # Apply data quality rules
    bri = "BRI001"
    cleaned_df = data_quality_rule_engine(df, bri)

    # Show results
    cleaned_df.show()
```

---

### **Key Adjustments**
1. **Column Name in Rule Parameters**:
   - The column name is now extracted from the `rule_parameters` JSON field.
   - Example: `{"column": "email", "regex": "^[^@]+@[^@]+$"}`.

2. **Flexible Rule Application**:
   - Rules are applied dynamically based on the `rule_type` and `rule_parameters`.

3. **Handling Non-Column Rules**:
   - Rules like `expect_table_row_count_to_be_between` and `expect_table_column_count_to_equal` do not require a column name.

---

### **Example Database Rules**
| id | bri       | rule_type                              | rule_parameters                          | action  | drq_group  |
|----|-----------|----------------------------------------|------------------------------------------|---------|------------|
| 1  | BRI001    | expect_column_values_to_match_regex    | {"column": "email", "regex": "^[^@]+@[^@]+$"} | reject  | DRQ_GROUP1 |
| 2  | BRI001    | expect_column_values_to_be_between     | {"column": "age", "min": 18, "max": 100} | log     | DRQ_GROUP1 |
| 3  | BRI001    | expect_column_values_to_be_within_last_n_days | {"column": "last_updated", "n": 30} | reject  | DRQ_GROUP1 |
| 4  | BRI001    | expect_table_row_count_to_be_between   | {"min_value": 1, "max_value": 1000}      | log     | DRQ_GROUP1 |
| 5  | BRI001    | expect_table_column_count_to_equal     | {"value": 3}                             | log     | DRQ_GROUP1 |

---

### **Output**
For the example above:
- Rows with invalid emails, ages below 18, or outdated `last_updated` values will be rejected or logged based on the rules.

---

Let me know if you need further adjustments!