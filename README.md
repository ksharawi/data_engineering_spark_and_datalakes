# data_engineering_spark_and_datalakes

1- Create an S3 bucket

![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/d403fbd6-724d-40e7-b27e-6a3a014c99a7)

2- Create folders to serve as the landing zone for the corresponding three different data sources:

![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/60b470b2-7645-4760-a4ec-00f64b169716)

3- Upload the data sets to the landing folders:

![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/ee40f1d8-9222-48c9-b6c9-9a94c2fdcf23)

4- Create Glue service role (including policies that allows it interact with S3 and other Glue resources):

![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/3c9f6a41-1f6d-4232-bc7c-3db85f2d3c38)

5- Discover the column's datatype for each data source using Glue Studio via a temporary job (in order to decide on the correct datatype when creating the corressponding Glue Tables):

![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/227a6a90-d644-48fd-ac84-42eb4bf14ed5)

This is an example for the configuration of the "Customer Landing" landing zone:
![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/a56fc8dc-e186-4d52-96cf-e77bf93b40a1)

6- Create a Glue Table for each of the landing zones to get a feel of the data (I have selected the datatypes based on the data sample preview for each landing zone in Glue Studio):

- customer_landing

  ![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/f723aeaf-2db4-444b-98e4-d95a14e9d0e2)

- step_trainer_landing

  ![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/6ee07cbf-8e58-4a2a-8e61-cb6db91ea7a6)

- accelerometer_landing

  ![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/d860eccb-8651-48a2-91d0-43bf43730b3b)

7- Create the trusted zones:

Building on top of what I learned during the course, I further simplified the process to generate accelerometer_trusted Glue table, buy combining the all elements in the same Glue job, and replacing the duplicate step of generating and referencing "Customer Trusted" with the use of a Spark SQL Query:

![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/b7595948-5c6d-4747-bcec-ec367ee3fc21)

I have also generated the accelerometer_trusted table on the fly by choosing:

![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/986f649e-eaf6-4d5d-a4c8-947d299326be)
![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/c9ef7220-315f-45af-a603-7c8ec180bcb6)

  

