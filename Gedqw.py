import pandas as pd
from pyspark.sql import SparkSession
from pydeequ.profiles import ColumnProfilerRunner
import json

def generate_dbt_ge_rules(spark_df, table_name, sample_size=1000000):
    """
    Generate data quality rules with both dbt and Great Expectations rule names
    
    Args:
        spark_df (pyspark.sql.DataFrame): Input DataFrame
        table_name (str): Name of the table
        sample_size (int): Sample size for profiling
        
    Returns:
        pd.DataFrame: Rule suggestions with dbt and GE rule names
    """
    # Sample the data if needed
    total_count = spark_df.count()
    if total_count > sample_size:
        spark_df = spark_df.sample(fraction=sample_size/total_count, seed=42)
    
    # Run column profiling
    result = ColumnProfilerRunner(spark_df.sql_ctx.sparkSession).onData(spark_df).run()
    profiles = result.profiles.toPandas().to_dict('records')
    
    # Get distinct counts for all columns
    distinct_counts = {col: spark_df.select(col).distinct().count() for col in spark_df.columns}
    
    # Prepare results dataframe
    rules = []
    
    for profile in profiles:
        column = profile['column']
        data_type = profile['dataType']
        completeness = profile['completeness']
        
        # 1. NULL Checks (for all columns)
        rules.append({
            "table_name": table_name,
            "column_name": column,
            "dbt_rule_name": "not_null",
            "ge_rule_name": "expect_column_values_to_not_be_null",
            "parameters": json.dumps({
                "column": column,
                "mostly": round(max(0.95, completeness * 0.95), 2)
            }),
            "description": f"Values in {column} should not be null"
        })
        
        # 2. Type-specific checks
        if data_type in ['Integral', 'Fractional']:
            # Numeric column rules
            min_val = profile['minimum']
            max_val = profile['maximum']
            mean_val = profile['mean']
            std_dev = profile['stdDev']
            
            # Range check
            rules.append({
                "table_name": table_name,
                "column_name": column,
                "dbt_rule_name": "relationships.range",
                "ge_rule_name": "expect_column_values_to_be_between",
                "parameters": json.dumps({
                    "column": column,
                    "min": min_val,
                    "max": max_val,
                    "strict_bounds": False,
                    "mostly": 0.95
                }),
                "description": f"Values in {column} should be between {min_val:.2f} and {max_val:.2f}"
            })
            
            # Zero/positive check if applicable
            if min_val >= 0:
                rules.append({
                    "table_name": table_name,
                    "column_name": column,
                    "dbt_rule_name": "expression_is_true",
                    "ge_rule_name": "expect_column_values_to_be_greater_than_or_equal_to",
                    "parameters": json.dumps({
                        "column": column,
                        "min_value": 0,
                        "mostly": 0.98
                    }),
                    "description": f"Values in {column} should be non-negative"
                })
                
        elif data_type == 'String':
            # String column rules
            min_length = profile['minLength']
            max_length = profile['maxLength']
            
            # Length check
            rules.append({
                "table_name": table_name,
                "column_name": column,
                "dbt_rule_name": "expression_is_true",
                "ge_rule_name": "expect_column_value_lengths_to_be_between",
                "parameters": json.dumps({
                    "column": column,
                    "min_value": min_length,
                    "max_value": max_length,
                    "mostly": 0.95
                }),
                "description": f"String length of {column} should be between {min_length} and {max_length}"
            })
            
            # Low cardinality check
            if distinct_counts[column] < 20:
                try:
                    histogram = json.loads(profile['histogram'])
                    value_set = [bucket['lowValue'] for bucket in histogram['fields']['buckets']]
                    
                    rules.append({
                        "table_name": table_name,
                        "column_name": column,
                        "dbt_rule_name": "accepted_values",
                        "ge_rule_name": "expect_column_values_to_be_in_set",
                        "parameters": json.dumps({
                            "column": column,
                            "values": value_set,
                            "mostly": 0.99
                        }),
                        "description": f"Values in {column} should be in {value_set}"
                    })
                except:
                    pass
                
        elif data_type in ['DateTime', 'Date']:
            # Datetime column rules
            min_date = profile['minimum']
            max_date = profile['maximum']
            
            # Date range check
            rules.append({
                "table_name": table_name,
                "column_name": column,
                "dbt_rule_name": "expression_is_true",
                "ge_rule_name": "expect_column_values_to_be_between",
                "parameters": json.dumps({
                    "column": column,
                    "min_value": min_date,
                    "max_value": max_date,
                    "mostly": 0.95
                }),
                "description": f"Dates in {column} should be between {min_date} and {max_date}"
            })
            
            # Future date check
            rules.append({
                "table_name": table_name,
                "column_name": column,
                "dbt_rule_name": "expression_is_true",
                "ge_rule_name": "expect_column_values_to_be_in_past",
                "parameters": json.dumps({
                    "column": column,
                    "mostly": 0.99
                }),
                "description": f"Dates in {column} should be in the past"
            })
    
    # 3. Table-level checks
    # Row count check
    rules.append({
        "table_name": table_name,
        "column_name": "ALL_COLUMNS",
        "dbt_rule_name": "row_count",
        "ge_rule_name": "expect_table_row_count_to_be_between",
        "parameters": json.dumps({
            "min_value": total_count * 0.8,
            "max_value": total_count * 1.2
        }),
        "description": f"Table {table_name} should have between {int(total_count * 0.8)} and {int(total_count * 1.2)} rows"
    })
    
    # Unique key check (if ID column exists)
    if any(col.lower() in ['id', 'pk'] for col in spark_df.columns):
        id_col = next((col for col in spark_df.columns if col.lower() in ['id', 'pk']), None)
        
        if id_col:
            rules.append({
                "table_name": table_name,
                "column_name": id_col,
                "dbt_rule_name": "unique",
                "ge_rule_name": "expect_column_values_to_be_unique",
                "parameters": json.dumps({
                    "column": id_col
                }),
                "description": f"Values in {id_col} should be unique"
            })
    
    # Convert to DataFrame
    columns = [
        "table_name",
        "column_name",
        "dbt_rule_name",
        "ge_rule_name",
        "parameters",
        "description"
    ]
    rules_df = pd.DataFrame(rules, columns=columns)
    
    return rules_df

# Example usage
if __name__ == "__main__":
    # Initialize Spark with PyDeequ
    spark = SparkSession.builder \
        .appName("DbtGERuleGenerator") \
        .config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.0-spark-3.1") \
        .getOrCreate()
    
    # Create sample data
    from pyspark.sql import functions as F
    df = spark.range(0, 100000) \
        .withColumn("customer_id", (F.rand() * 10000).cast("int")) \
        .withColumn("amount", F.rand() * 1000) \
        .withColumn("category", 
                   F.when(F.rand() > 0.8, "A")
                    .when(F.rand() > 0.6, "B")
                    .otherwise("C")) \
        .withColumn("transaction_date", F.date_add(F.current_date(), (F.rand() * 365 - 180).cast("int"))) \
        .withColumn("is_active", F.when(F.rand() > 0.3, True).otherwise(False))
    
    # Generate rule suggestions
    rules_df = generate_dbt_ge_rules(df, "transactions")
    
    # Display results
    pd.set_option('display.max_colwidth', None)
    print(rules_df.head(10))
    
    # Save to CSV
    rules_df.to_csv("dbt_ge_rules.csv", index=False)
    print("\nRules saved to dbt_ge_rules.csv")

    # Save to Excel with separate sheets for dbt and GE
    with pd.ExcelWriter('dbt_ge_rules.xlsx') as writer:
        # dbt rules
        dbt_rules = rules_df[['table_name', 'column_name', 'dbt_rule_name', 'parameters', 'description']]
        dbt_rules.to_excel(writer, sheet_name='dbt_rules', index=False)
        
        # GE rules
        ge_rules = rules_df[['table_name', 'column_name', 'ge_rule_name', 'parameters', 'description']]
        ge_rules.to_excel(writer, sheet_name='ge_rules', index=False)
    
    print("Rules also saved to dbt_ge_rules.xlsx with separate sheets")