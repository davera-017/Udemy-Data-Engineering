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

# Script generated for node Landing step-trainer zone
Landingsteptrainerzone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://cd0030bucket-daniel-avila/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Landingsteptrainerzone_node1",
)

# Script generated for node Curated customer zone
Curatedcustomerzone_node1673310283518 = glueContext.create_dynamic_frame.from_catalog(
    database="default2",
    table_name="customer_curated",
    transformation_ctx="Curatedcustomerzone_node1673310283518",
)

# Script generated for node Customer privacy and accelerometer filter
Customerprivacyandaccelerometerfilter_node2 = Join.apply(
    frame1=Landingsteptrainerzone_node1,
    frame2=Curatedcustomerzone_node1673310283518,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="Customerprivacyandaccelerometerfilter_node2",
)

# Script generated for node Drop Fields
DropFields_node1673310351525 = DropFields.apply(
    frame=Customerprivacyandaccelerometerfilter_node2,
    paths=[
        "serialnumber",
        "birthday",
        "timestamp",
        "sharewithpublicasofdate",
        "sharewithresearchasofdate",
        "registrationdate",
        "customername",
        "email",
        "lastupdatedate",
        "phone",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1673310351525",
)

# Script generated for node Trusted step-trainer zone
Trustedsteptrainerzone_node3 = glueContext.getSink(
    path="s3://cd0030bucket-daniel-avila/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="Trustedsteptrainerzone_node3",
)
Trustedsteptrainerzone_node3.setCatalogInfo(
    catalogDatabase="default2", catalogTableName="step_trainer_trusted"
)
Trustedsteptrainerzone_node3.setFormat("json")
Trustedsteptrainerzone_node3.writeFrame(DropFields_node1673310351525)
job.commit()