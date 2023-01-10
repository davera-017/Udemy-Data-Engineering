import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Landing customer zone
Landingcustomerzone_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="default2",
    table_name="customer_landing",
    transformation_ctx="Landingcustomerzone_node1",
)

# Script generated for node Privacy filter
Privacyfilter_node2 = Filter.apply(
    frame=Landingcustomerzone_node1,
    f=lambda row: (not (row["sharewithresearchasofdate"] == 0)),
    transformation_ctx="Privacyfilter_node2",
)

# Script generated for node Trusted Customer zone
TrustedCustomerzone_node3 = glueContext.getSink(
    path="s3://cd0030bucket-daniel-avila/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="TrustedCustomerzone_node3",
)
TrustedCustomerzone_node3.setCatalogInfo(
    catalogDatabase="default2", catalogTableName="customer_trusted"
)
TrustedCustomerzone_node3.setFormat("json")
TrustedCustomerzone_node3.writeFrame(Privacyfilter_node2)
job.commit()
