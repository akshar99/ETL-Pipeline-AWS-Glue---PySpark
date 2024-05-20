/*
This is a sql cript to run in AWS Athena for 
creating an external table that we can query.
*/

CREATE EXTERNAL TABLE ny_cars (
  `new&used` STRING,
  name STRING,
  money INT,
  `Exterior color` STRING,
  Drivetrain STRING,
  `Fuel type` STRING,
  Transmission STRING,
  Engine STRING,
  Mileage STRING,
  Safety STRING,
  brand STRING,
  Year DATE,
  Model STRING,
  currency STRING,
  MPG INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ','
) LOCATION 's3://etl-spark-job-dest/final_output/'
TBLPROPERTIES ('has_encrypted_data'='false');
