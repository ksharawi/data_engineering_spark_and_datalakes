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

  ![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/f055ef01-ceef-47b0-81f1-a2abfa7323e0)

- accelerometer_landing

  ![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/d860eccb-8651-48a2-91d0-43bf43730b3b)

  ![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/f27d092c-a372-4ee1-baf4-75226c47a781)

- step_trainer_landing

  ![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/6ee07cbf-8e58-4a2a-8e61-cb6db91ea7a6)

  ![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/e8f4f9f5-7efd-4d07-ab05-f9d8a706d423)

7- Create the trusted zones:

Building on top of what I learned during the course, I further simplified the process to generate the trusted zones, by combining all elements in the same Glue job, and replacing the duplicate step of generating and referencing "Customer Trusted" and the join with the use of a  customised Spark SQL Query in its core (3 steps in 1!):

![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/7309a620-c3f8-4084-8aef-f05eca6c8155)

![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/54b47921-14f5-465e-b405-d73fea05f24a)

I have also generated the accelerometer_trusted table on the fly by choosing:

![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/986f649e-eaf6-4d5d-a4c8-947d299326be)

on the creation of the accelerometer trusted zone, and similarly I did for step_trainer_trusted.

- customer_trusted

  ![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/b603287d-afd6-403e-a1df-abcdcf4f5a12)


- accelerometer_trusted
  
  ![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/d2ea501f-7f28-4a6d-96a8-2f8df3ea7fe6)


- step_trainer_trusted

  ![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/aabc2b91-4400-4c79-a68f-50204824380d)

  ![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/adaea0d9-d806-4b65-a919-ba25f0162c64)

- customer_curated

  ![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/796f27b6-8c61-4b6f-aa50-12ff93b38420)

  ![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/b3fe3e94-3d6f-40e4-a8f2-e2aa5a2afea7)

- machine_learning_curated

  ![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/cfb33dee-a527-4774-8d5c-12cdd2608776)

  ![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/8e8e020b-61b6-4dc4-bb5b-33f8ac8b3bd8)

  ![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/07df422a-ef74-4ad4-84f8-f7885937cd96)

# Bonus!

- Return records for those only made after the customer has provided consent:

  ![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/563c2622-1237-41f5-a92b-52af7ab50742)

- Anonymise customer sensitive data in the curated tables:

  - machine_learning_curated

    ![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/434da93c-1917-452e-9172-58da84aacb5a)

  - customer_curated

    ![image](https://github.com/ksharawi/data_engineering_spark_and_datalakes/assets/94605032/8a44e619-d621-407a-b0e1-a74d5ce6942e)






  


  

