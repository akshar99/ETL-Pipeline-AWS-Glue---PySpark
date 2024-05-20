import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()            
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark1 = SparkSession.builder.appName('Spark ETL').getOrCreate()
source = "s3://etl-spark-job/New_York_cars.csv"

#UDF to perform transformation
def transform_data_udf(path, spark1):
    # Read data
    df = spark1.read.option("header", 'true').csv(path).cache()

    # Drop unwanted columns
    columns_to_drop = ['Convenience', 'Interior color', 'Entertainment', 'Exterior', 'Seating', 'Accidents or damage', 'Clean title', '1-owner vehicle', 'Personal use only']
    for ele in columns_to_drop:
        df = df.drop(ele)

    # Drop rows with missing values
    df = df.na.drop()

    # Convert 'money' and 'Year' columns to appropriate types
    df = df.withColumn('money', df['money'].cast('int'))
    df = df.withColumn('Year', df['Year'].cast('string'))

    # Convert 'Year' to date format
    df = df.withColumn("Year", F.to_date(F.concat(df["Year"], F.lit("-01-01"))))

    # Split 'MPG' column and calculate average
    mpg_split = F.split(df['MPG'], "-")
    mpg_low = F.regexp_extract(df["MPG"], r'(\d+)', 0).cast("int")
    mpg_high = F.regexp_extract(df["MPG"], r'(\d+)', 1).cast("int")
    df = df.withColumn("MPG_average", ((mpg_low + mpg_high) / 2).cast("int"))
    df = df.drop('MPG').withColumnRenamed('MPG_average', 'MPG')

    return df

df1 = transform_data_udf(source, spark1)

# Repartition DataFrame to have only one partition
df1_single_partition = df1.repartition(1)

# Write the data in the DataFrame to a single CSV file in Amazon S3
df1_single_partition.write.option("header", "true").csv("s3://etl-spark-job-dest/final_output_csv")

job.commit()
