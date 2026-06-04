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

# Script generated for node Step_Trainer Trusted
Step_TrainerTrusted_node1780587977188 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-sgdp-wh/step_trainer/trusted/"], "recurse": True}, transformation_ctx="Step_TrainerTrusted_node1780587977188")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1780587923957 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-sgdp-wh/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1780587923957")

# Script generated for node Join accelerometer trusted to step_trainer trusted
SqlQuery3802 = '''
SELECT 
   at.*
  ,stt.*
FROM at 
INNER JOIN stt 
ON at.timestamp = stt.sensorreadingtime
'''
Joinaccelerometertrustedtostep_trainertrusted_node1780588075265 = sparkSqlQuery(glueContext, query = SqlQuery3802, mapping = {"stt":Step_TrainerTrusted_node1780587977188, "at":AccelerometerTrusted_node1780587923957}, transformation_ctx = "Joinaccelerometertrustedtostep_trainertrusted_node1780588075265")

# Script generated for node ml_curated
EvaluateDataQuality().process_rows(frame=Joinaccelerometertrustedtostep_trainertrusted_node1780588075265, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1780586902966", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
ml_curated_node1780588298706 = glueContext.getSink(path="s3://stedi-lake-house-sgdp-wh/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="ml_curated_node1780588298706")
ml_curated_node1780588298706.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="step_trainer_ml_curated")
ml_curated_node1780588298706.setFormat("json")
ml_curated_node1780588298706.writeFrame(Joinaccelerometertrustedtostep_trainertrusted_node1780588075265)
job.commit()