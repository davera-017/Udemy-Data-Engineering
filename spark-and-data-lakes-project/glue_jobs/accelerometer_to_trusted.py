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

# Script generated for node Landing accelerometer zone
Landingaccelerometerzone_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="default2",
    table_name="accelerometer_landing",
    transformation_ctx="Landingaccelerometerzone_node1",
)

# Script generated for node Trusted customer zone
Trustedcustomerzone_node1673291245765 = glueContext.create_dynamic_frame.from_catalog(
    database="default2",
    table_name="customer_trusted",
    transformation_ctx="Trustedcustomerzone_node1673291245765",
)

# Script generated for node Customer privacy filter
Customerprivacyfilter_node2 = Join.apply(
    frame1=Landingaccelerometerzone_node1,
    frame2=Trustedcustomerzone_node1673291245765,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Customerprivacyfilter_node2",
)

# Script generated for node Drop Fields
DropFields_node1673291336854 = DropFields.apply(
    frame=Customerprivacyfilter_node2,
    paths=[
        "serialnumber",
        "sharewithpublicasofdate",
        "birthday",
        "registrationdate",
        "sharewithresearchasofdate",
        "customername",
        "email",
        "lastupdatedate",
        "phone",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1673291336854",
)

# Script generated for node Trusted accelerometer zone
Trustedaccelerometerzone_node3 = glueContext.getSink(
    path="s3://cd0030bucket-daniel-avila/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="Trustedaccelerometerzone_node3",
)
Trustedaccelerometerzone_node3.setCatalogInfo(
    catalogDatabase="default2", catalogTableName="accelerometer_trusted"
)
Trustedaccelerometerzone_node3.setFormat("json")
Trustedaccelerometerzone_node3.writeFrame(DropFields_node1673291336854)
job.commit()
