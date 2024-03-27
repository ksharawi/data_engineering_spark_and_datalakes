# data_engineering_spark_and_datalakes

1- Create an S3 bucket

![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/d403fbd6-724d-40e7-b27e-6a3a014c99a7)

2- Create folders to serve as the landing zone for the corresponding three different data sources:

![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/60b470b2-7645-4760-a4ec-00f64b169716)

3- Upload the data sets to the landing folders:

![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/ee40f1d8-9222-48c9-b6c9-9a94c2fdcf23)

4- Create Glue service role (including policies that allows it interact with S3 and other Glue resources):

![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/3c9f6a41-1f6d-4232-bc7c-3db85f2d3c38)

5- Using Glue Studio create the landing zones from the S3 bucket folders:

![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/227a6a90-d644-48fd-ac84-42eb4bf14ed5)

This is an example for the configuration of the "Customer (Mobile) Landing" landing zone:
![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/a56fc8dc-e186-4d52-96cf-e77bf93b40a1)

6- Create a Glue Table for each of the landing zones to get a feel of the data:

---CREATE EXTERNAL TABLE IF NOT EXISTS `stedi-step-trainer`.`customer_landing` (
  `customername` string,
  `email` string,
  `phone` string,
  `birthday` string,
  `serialnumber` string,
  `registrationdate` bigint,
  `sharewithpublicasofdate` bigint,
  `sharewithresearchasofdate` bigint,
  `lastupdatedate` bigint,
  `sharewithfriendsasofdate` bigint
) COMMENT "Data from the mobile application."
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-step-trainer/customer_landing/'
TBLPROPERTIES ('classification' = 'json');
