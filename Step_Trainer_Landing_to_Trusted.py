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

# Script generated for node Step_Trainer Landed
Step_TrainerLanded_node1780434125519 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-sgdp-wh/step_trainer/landing/"], "recurse": True}, transformation_ctx="Step_TrainerLanded_node1780434125519")

# Script generated for node Customer Curated
CustomerCurated_node1780434312936 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-sgdp-wh/customer/curated/"], "recurse": True}, transformation_ctx="CustomerCurated_node1780434312936")

# Script generated for node Select serialnumber
SqlQuery3262 = '''
SELECT serialnumber AS cust_curated_serialnumber
FROM myDataSource
'''
Selectserialnumber_node1780434444412 = sparkSqlQuery(glueContext, query = SqlQuery3262, mapping = {"myDataSource":CustomerCurated_node1780434312936}, transformation_ctx = "Selectserialnumber_node1780434444412")

# Script generated for node Join step_trainer to curated_customer
Joinstep_trainertocurated_customer_node1780434513702 = Join.apply(frame1=Step_TrainerLanded_node1780434125519, frame2=Selectserialnumber_node1780434444412, keys1=["serialnumber"], keys2=["cust_curated_serialnumber"], transformation_ctx="Joinstep_trainertocurated_customer_node1780434513702")

# Script generated for node Step_Trainer Trusted
EvaluateDataQuality().process_rows(frame=Joinstep_trainertocurated_customer_node1780434513702, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1780434094018", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Step_TrainerTrusted_node1780434703484 = glueContext.getSink(path="s3://stedi-lake-house-sgdp-wh/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Step_TrainerTrusted_node1780434703484")
Step_TrainerTrusted_node1780434703484.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="step_trainer_trusted")
Step_TrainerTrusted_node1780434703484.setFormat("json")
Step_TrainerTrusted_node1780434703484.writeFrame(Joinstep_trainertocurated_customer_node1780434513702)
job.commit()