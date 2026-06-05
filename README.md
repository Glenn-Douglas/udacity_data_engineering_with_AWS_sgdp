This repository is for submission of project three (Spark and Data Lakes) within Udacity's 'Data Engineering with AWS' Nanodegree program.

The project involved the construction of a data lake using the following tools: AWS Glue Studio, AWS Athena, and AWS s3.
The solution was built with source sensor data for the data science team with the ultimate goal of training a machine learning model.

Landing data: The raw data is pulled from and stored in s3. The code defining the landing tables can be found within the files: 
customer_landing_ddl.sql, accelerometer_landing.sql, and step_trainer_landing_ddl.sql 

Three python files remove PII (our silver tables): 
Customer_Landing_to_Trusted.py, Accelerometer_Landing_to_Trusted.py, and Step_Trainer_Landing_to_Trusted.py

Our final python file creates the gold dataset to be provided to data science:
Step_Trainer_ML_Curated.py

Source data was pulled from the Udacity course's github: https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/tree/main
SQL DDL files were created in AWS Athena.
Python files were created within Glue Studio and the scripts uploaded to this location. 
Validation queries were run and screenshot from Athena.

This repository is not to be used for any student's submission within the nanodegree program other than my own. It is available for reference 
with proper citation.
