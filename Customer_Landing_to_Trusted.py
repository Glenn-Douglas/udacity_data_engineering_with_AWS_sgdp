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

# Script generated for node Customers Landing
CustomersLanding_node1779981892567 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-sgdp-wh/customer/landing/"], "recurse": True}, transformation_ctx="CustomersLanding_node1779981892567")

# Script generated for node Share with Research
SqlQuery2156 = '''
select * from myDataSource
where shareWithResearchAsOfDate IS NOT NULL;
'''
SharewithResearch_node1779982011457 = sparkSqlQuery(glueContext, query = SqlQuery2156, mapping = {"myDataSource":CustomersLanding_node1779981892567}, transformation_ctx = "SharewithResearch_node1779982011457")

# Script generated for node Customers Trusted
EvaluateDataQuality().process_rows(frame=SharewithResearch_node1779982011457, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1779981836341", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomersTrusted_node1779982144028 = glueContext.getSink(path="s3://stedi-lake-house-sgdp-wh/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomersTrusted_node1779982144028")
CustomersTrusted_node1779982144028.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customer_trusted")
CustomersTrusted_node1779982144028.setFormat("json")
CustomersTrusted_node1779982144028.writeFrame(SharewithResearch_node1779982011457)
job.commit()