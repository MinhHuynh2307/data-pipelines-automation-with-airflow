# Data Pipeline Automation With Airflow

## 1. Project description
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

I am going to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills using Airflow. Due to the fact that the data quality plays a big part when analyses are executed on top the data warehouse and analysts want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

<center>
<img style="float: center;height:450px;" src="dag.png"><br><br>
DAG
</center>

## 2. Directories and Files

**`dags`** This directory contains 02 following files:
- **`final_project_sql_statements.py`** A Python file will be used to transform data in Redshift Severless and insert transformed data to fact and dimension table.

- **`final_project.py`** A Python file will be imported to Airflow to run a DAG.
 
**`plugins`** This directory contains 04 following files:
- **`data_quality.py`** A Python file that defines the operator that is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually. For example one test could be a SQL statement that checks if certain column contains NULL values by counting all the rows that have NULL in the column. We do not want to have any NULLs so expected result would be 0 and the test would compare the SQL statement's outcome to the expected result.

- **`load_dimension.py`** A Python file that defines the operator that utilize `SqlQueries` class in `final_project_sql_statements.py` to run data transformations. Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement and target database on which to run the query against. 

- **`load_fact.py`** A Python file that defines the operator that utilize `SqlQueries` class in `final_project_sql_statements.py` to run data transformations. Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement and target database on which to run the query against.

- **`stage_redshift.py`** A Python file that defines the operator that is expected to be able to load any JSON formatted files from S3 to Amazon Redshift. 

**`create_tables.sql`** SQL queries that create tables in Redshift Severless.

**`dag.png`** DAG graph

