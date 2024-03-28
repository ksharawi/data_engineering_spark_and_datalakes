import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1711583480399 = glueContext.create_dynamic_frame.from_catalog(database="stedi-step-trainer", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1711583480399")

# Script generated for node Customer Curated
CustomerCurated_node1711583461161 = glueContext.create_dynamic_frame.from_catalog(database="stedi-step-trainer", table_name="customer_curated", transformation_ctx="CustomerCurated_node1711583461161")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1711583501979 = glueContext.create_dynamic_frame.from_catalog(database="stedi-step-trainer", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1711583501979")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT stt.sensorReadingTime, stt.serialNumber,
stt.distanceFromObject, at.user, at.x, at.y, at.z
FROM step_trainer_trusted stt
JOIN accelerometer_trusted at ON (
    stt.sensorReadingTime = at.timeStamp
    )
JOIN customer_curated cc ON (
    at.user = cc.email
    )
WHERE 
    stt.sensorReadingTime >= cc.sharewithresearchasofdate
;
'''
SQLQuery_node1711583521515 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_trusted":StepTrainerTrusted_node1711583501979, "customer_curated":CustomerCurated_node1711583461161, "accelerometer_trusted":AccelerometerTrusted_node1711583480399}, transformation_ctx = "SQLQuery_node1711583521515")

# Script generated for node Drop Fields
DropFields_node1711585189659 = DropFields.apply(frame=SQLQuery_node1711583521515, paths=["user"], transformation_ctx="DropFields_node1711585189659")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1711583981196 = glueContext.getSink(path="s3://stedi-step-trainer-data/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1711583981196")
MachineLearningCurated_node1711583981196.setCatalogInfo(catalogDatabase="stedi-step-trainer",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1711583981196.setFormat("json")
MachineLearningCurated_node1711583981196.writeFrame(DropFields_node1711585189659)
job.commit()
