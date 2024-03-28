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
AccelerometerTrusted_node1711578795163 = glueContext.create_dynamic_frame.from_catalog(database="stedi-step-trainer", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1711578795163")

# Script generated for node Customer Trusted
CustomerTrusted_node1711578784431 = glueContext.create_dynamic_frame.from_catalog(database="stedi-step-trainer", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1711578784431")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT * FROM customerTrusted ct
JOIN (
    SELECT DISTINCT user FROM accelerometerTrusted
) at ON (ct.email = at.user)
;
'''
SQLQuery_node1711578836713 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customerTrusted":CustomerTrusted_node1711578784431, "accelerometerTrusted":AccelerometerTrusted_node1711578795163}, transformation_ctx = "SQLQuery_node1711578836713")

# Script generated for node Drop Fields
DropFields_node1711585954201 = DropFields.apply(frame=SQLQuery_node1711578836713, paths=["email", "customerName", "phone", "birthDay"], transformation_ctx="DropFields_node1711585954201")

# Script generated for node Customer Curated
CustomerCurated_node1711579012434 = glueContext.getSink(path="s3://stedi-step-trainer-data/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1711579012434")
CustomerCurated_node1711579012434.setCatalogInfo(catalogDatabase="stedi-step-trainer",catalogTableName="customer_curated")
CustomerCurated_node1711579012434.setFormat("json")
CustomerCurated_node1711579012434.writeFrame(DropFields_node1711585954201)
job.commit()
