import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Trusted customer zone
Trustedcustomerzone_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="default2",
    table_name="customer_trusted",
    transformation_ctx="Trustedcustomerzone_node1",
)

# Script generated for node Landing accelerometer zone
Landingaccelerometerzone_node1673292171035 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="default2",
        table_name="accelerometer_landing",
        transformation_ctx="Landingaccelerometerzone_node1673292171035",
    )
)

# Script generated for node Accelerometer user filter
Accelerometeruserfilter_node2 = Join.apply(
    frame1=Trustedcustomerzone_node1,
    frame2=Landingaccelerometerzone_node1673292171035,
    keys1=["customername"],
    keys2=["user"],
    transformation_ctx="Accelerometeruserfilter_node2",
)

# Script generated for node Filter
Filter_node1673292278205 = DropFields.apply(
    frame=Accelerometeruserfilter_node2,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="Filter_node1673292278205",
)

# Script generated for node Curated customer zone
Curatedcustomerzone_node3 = glueContext.getSink(
    path="s3://cd0030bucket-daniel-avila/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="Curatedcustomerzone_node3",
)
Curatedcustomerzone_node3.setCatalogInfo(
    catalogDatabase="default2", catalogTableName="customer_curated"
)
Curatedcustomerzone_node3.setFormat("json")
Curatedcustomerzone_node3.writeFrame(Filter_node1673292278205)
job.commit()
