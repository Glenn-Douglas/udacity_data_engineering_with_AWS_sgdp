import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1780246605587 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-sgdp-wh/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1780246605587")

# Script generated for node Customer Trusted
CustomerTrusted_node1780246453481 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-sgdp-wh/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1780246453481")

# Script generated for node Join customer trusted to accelerometer landing
Joincustomertrustedtoaccelerometerlanding_node1780246672564 = Join.apply(frame1=AccelerometerLanding_node1780246605587, frame2=CustomerTrusted_node1780246453481, keys1=["user"], keys2=["email"], transformation_ctx="Joincustomertrustedtoaccelerometerlanding_node1780246672564")

# Script generated for node Drop customer fields
SqlQuery2432 = '''
select
   user
  ,timestamp
  ,x   
  ,y  
  ,z
from myDataSource
'''
Dropcustomerfields_node1780247399157 = sparkSqlQuery(glueContext, query = SqlQuery2432, mapping = {"myDataSource":Joincustomertrustedtoaccelerometerlanding_node1780246672564}, transformation_ctx = "Dropcustomerfields_node1780247399157")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Dropcustomerfields_node1780247399157, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1780244670395", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1780246846231 = glueContext.getSink(path="s3://stedi-lake-house-sgdp-wh/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1780246846231")
AmazonS3_node1780246846231.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="accelerometer_trusted")
AmazonS3_node1780246846231.setFormat("json")
AmazonS3_node1780246846231.writeFrame(Dropcustomerfields_node1780247399157)
job.commit()