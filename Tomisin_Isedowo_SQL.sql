-- Databricks notebook source
-- MAGIC %python
-- MAGIC #CHECK THE CONTENT OF THE DBFS
-- MAGIC dbutils.fs.ls("/FileStore/tables")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #declare a variable called fileroot
-- MAGIC fileroot = "clinicaltrial_2023"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import os
-- MAGIC os.environ['fileroot'] = fileroot

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Reads the CSV file into an RDD
-- MAGIC myRDD1 = sc.textFile("/FileStore/tables/"+fileroot+".csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #THIS COMMAND ASSISTS IN DEFINING A FUNCTION DEDICATED TO CLEANING AND TRANSFORMING INDIVIDUAL ROWS WITHIN THE CLINICALTRIAL DATASET
-- MAGIC def clean_and_transform(row):
-- MAGIC     #THIS AIDS IN ELIMINATING SURPLUS TRAILING COMMAS AND UNNECESARY DOUBLE QUOTES THROUGH THE UTILIZATION OF THE STRIP METHOD.
-- MAGIC     cleaned_row = row.strip(",").strip('"')
-- MAGIC     #THIS COMMAND FACILITATES THE SPLITTING OF THE ROW BY THE SPECIFIED DELIMITER, WHICH IN THIS CASE IS THE TAB ('\t').
-- MAGIC     cleaned_row = cleaned_row.split('\t')
-- MAGIC
-- MAGIC     return cleaned_row
-- MAGIC
-- MAGIC # RDD containing rows with varying column lengths
-- MAGIC  
-- MAGIC def fill_empty_columns(row):
-- MAGIC     # Check if the row has less than 14 columns
-- MAGIC     if len(row) < 14:
-- MAGIC         # Fill up the remaining columns with empty strings
-- MAGIC         row += [''] * (14 - len(row))
-- MAGIC     return row

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #THIS COMMAND MAP THE CLEAN_AND _TRANSFORM FUNCTION TO myRDDS.
-- MAGIC myRDD2 = myRDD1.map(clean_and_transform) \
-- MAGIC         .map(fill_empty_columns)\
-- MAGIC         .zipWithIndex().filter(lambda x: x[1] > 0).map(lambda x: x[0])
-- MAGIC
-- MAGIC myRDD2.take(5)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Function to replace empty strings with None
-- MAGIC def replace_empty_with_null(line):
-- MAGIC     return [None if field == "" else field for field in line]
-- MAGIC  
-- MAGIC # Replace empty fields with null values
-- MAGIC replace_with_null = myRDD2.map(replace_empty_with_null)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def clean_date(date):
-- MAGIC     if len(date) < 8:
-- MAGIC         date = date + "-01"
-- MAGIC     return date
-- MAGIC  
-- MAGIC def fill_empty_date(date, default_value):
-- MAGIC     return default_value if not date else date
-- MAGIC  
-- MAGIC # Select the column indices
-- MAGIC column_indices = [12, 13]
-- MAGIC  
-- MAGIC # Clean up the start and completion dates
-- MAGIC default_date = "2024-01-01"
-- MAGIC  
-- MAGIC # First RDD: Fill up empty date columns
-- MAGIC myRDD3 = replace_with_null.map(lambda row: [
-- MAGIC     fill_empty_date(row[i], default_date) if i in column_indices else row[i] for i in range(len(row))
-- MAGIC ])
-- MAGIC  
-- MAGIC # Second RDD: Format the date columns
-- MAGIC myRDD4 = myRDD3.map(lambda row: [
-- MAGIC     clean_date(row[i]) if i in column_indices else row[i] for i in range(len(row))
-- MAGIC ])
-- MAGIC  
-- MAGIC myRDD4.take(5)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import*
-- MAGIC
-- MAGIC mySchema = StructType([
-- MAGIC            StructField("Id", StringType()),
-- MAGIC            StructField("Study Title", StringType()),
-- MAGIC            StructField("Acronym", StringType()),
-- MAGIC            StructField("Status", StringType()),
-- MAGIC            StructField("Conditions", StringType()),
-- MAGIC            StructField("Interventions", StringType()),
-- MAGIC            StructField("Sponsor", StringType()),
-- MAGIC            StructField("Collaborators", StringType()),
-- MAGIC            StructField("Enrollment", StringType()),
-- MAGIC            StructField("Funder Type", StringType()),
-- MAGIC            StructField("Type", StringType()),
-- MAGIC            StructField("Study Design", StringType()),
-- MAGIC            StructField("Start", StringType()),
-- MAGIC             StructField("Completion", StringType()),
-- MAGIC ])
-- MAGIC Data =spark.createDataFrame(myRDD4, mySchema)
-- MAGIC
-- MAGIC Data.createOrReplaceTempView('clinicaltrial_2023')
-- MAGIC
-- MAGIC # CREATE OR REPLACE TEMPORARY VIEW distinct_study_counts 

-- COMMAND ----------

--Question 1
-- This SQL query counts the distinct values of the Id column in the clinical_trials table
-- and aliases the result as distinct_studies_count.
 
SELECT COUNT(DISTINCT Id) AS distinct_studies_count
FROM clinicaltrial_2023;

-- COMMAND ----------

-- Question 2
SELECT
    Type,
    COUNT(*) AS count
FROM
    clinicaltrial_2023
GROUP BY
    Type
ORDER BY
    count DESC;

-- COMMAND ----------

--Question 3
-- It first line explodes the 'Conditions' column by '|' delimiter, to separate each condition and filters out non-empty and non-null values.
-- Then, it counts the occurrences of each condition and groups the results by condition.
-- Finally, it selects the top 5 conditions based on the count of occurrences, sorting them in descending order.
WITH condition_counts AS (
    SELECT
        condition,
        COUNT(*) AS count
    FROM (
        SELECT
            Id,
            explode(split(Conditions, '\\|')) AS condition
        FROM
            clinicaltrial_2023
        WHERE
            Conditions IS NOT NULL AND Conditions != ''
    ) exploded_conditions
    GROUP BY
        condition
)
SELECT
    condition,
    count
FROM
    condition_counts
ORDER BY
    count DESC
LIMIT 5;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/FileStore/tables/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #This should run on Python cell
-- MAGIC  
-- MAGIC # Load data from CSV into a temporary view
-- MAGIC spark.read.csv("/FileStore/tables/pharma.csv", header=True, inferSchema=True).createOrReplaceTempView("pharma")

-- COMMAND ----------

--Q4 IMPLEMENTATION IN SQL
-- Step 3: Left Anti Join to find entries that exist in Clinical Trial Sponsor but not in Pharma Companies
CREATE OR REPLACE TEMP VIEW filtered_df AS
SELECT
    c.Sponsor,
    COUNT(*) AS count
FROM
   clinicaltrial_2023 c
LEFT ANTI JOIN
     pharma p ON c.Sponsor = p.Parent_company
GROUP BY
  c.Sponsor;
 
-- Step 4: Sort the result in descending order of counts
CREATE OR REPLACE TEMP VIEW sorted_result_df AS
SELECT
    Sponsor,
    count
FROM
    filtered_df
ORDER BY
    count DESC;
 
-- Step 5: Take the top 10 records
CREATE OR REPLACE TEMP VIEW top_10_sponsors AS
SELECT
    Sponsor,
    count
FROM
    sorted_result_df
LIMIT 10;
 
-- Show the top 10 sponsors
SELECT * FROM top_10_sponsors;

-- COMMAND ----------

--Q5 IMPLEMENTATION IN SQL
-- Aggregate completed studies count for each month
CREATE OR REPLACE TEMP VIEW completed_studies AS
SELECT
    MONTH(Completion) AS month,
    COUNT(*) AS completed_count
FROM
    clinicaltrial_2023
WHERE
    STATUS = 'COMPLETED'
    AND YEAR(Completion) = 2023
GROUP BY
    MONTH(Completion)
ORDER BY
    month;
 
SELECT * from completed_studies

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Q5 IMPLEMENTATION IN SQL(PLOTTING OF THE GRAPH)
-- MAGIC
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC import calendar
-- MAGIC  
-- MAGIC #Query the completed_studies view and convert the result into a DataFrame
-- MAGIC completed_studies_df = spark.sql("SELECT * FROM completed_studies")
-- MAGIC  
-- MAGIC #Collect the results to the driver as a list of Row objects
-- MAGIC completed_studies_rows = completed_studies_df.collect()
-- MAGIC  
-- MAGIC #Convert the result to a dictionary
-- MAGIC completed_studies_by_month_dict = {row['month']: row['completed_count'] for row in completed_studies_rows}
-- MAGIC  
-- MAGIC #Plotting
-- MAGIC months = range(1, 13)
-- MAGIC study_counts = [completed_studies_by_month_dict.get(month, 0) for month in months]
-- MAGIC  
-- MAGIC #Convert month numbers to month names
-- MAGIC month_names = [calendar.month_name[month] for month in months]
-- MAGIC  
-- MAGIC plt.figure(figsize=(12, 6))
-- MAGIC plt.bar(month_names, study_counts, color='skyblue')
-- MAGIC plt.title('Number of Completed Studies for Each Month in 2023')
-- MAGIC plt.xlabel('Month')
-- MAGIC plt.ylabel('Number of Completed Studies')
-- MAGIC plt.xticks(rotation=45, ha='right')  # Rotate the x-axis labels for better readability
-- MAGIC plt.grid(axis='y', linestyle='--', alpha=0.7)
-- MAGIC plt.tight_layout()
-- MAGIC plt.show()
-- MAGIC  
-- MAGIC #Table of values
-- MAGIC print("Month\t\t\tCompleted Studies")
-- MAGIC for month in range(1, 13):
-- MAGIC     count = completed_studies_by_month_dict.get(month, 0)
-- MAGIC     print(f"{calendar.month_name[month]:<24}{count}")
