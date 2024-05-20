ETL Project: Creating a Data Pipeline with AWS Glue and PySpark
This project focuses on building an Extract, Transform, Load (ETL) pipeline using AWS Glue and PySpark for data transformation. The process involves several key steps:

1. Setting up AWS Glue Role and Policies
To establish a connection between AWS Glue and Amazon S3, it's necessary to create a role and attach the appropriate policies to it. A custom policy is also required. See the attached image for the policies associated with the role named "ETL-GLUE" and the JSON code for the custom policy.

2. Developing the ETL Script
The ETL process is executed through a Glue Job, utilizing PySpark for data transformation. The script, named ETL_Script.py, orchestrates the transformation process to prepare the data for analysis.

3. Creating a Glue Catalog Database
A Catalog database is created within AWS Glue to serve as a central repository for storing metadata. This metadata plays a crucial role in integrating with other AWS services, facilitating seamless data management and analysis.

4. Transformation of Data
The provided dataset, New_York_cars.zip, undergoes transformation as part of the ETL process. This transformed data is essential for subsequent analysis and insights generation.

5. Data Analysis with Pandas, Matplotlib, and Seaborn
Further analysis of the transformed data is conducted using popular Python libraries such as Pandas, Matplotlib, and Seaborn. The Jupyter Notebook Data Analysis.ipynb showcases various analytical techniques and visualizations applied to the dataset.
