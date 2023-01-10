import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkAggregate(
    glueContext, parentFrame, groups, aggs, transformation_ctx
) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = (
        parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs)
        if len(groups) > 0
        else parentFrame.toDF().agg(*aggsFuncs)
    )
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Trusted step-trainer zone
Trustedsteptrainerzone_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="default2",
    table_name="step_trainer_trusted",
    transformation_ctx="Trustedsteptrainerzone_node1",
)

# Script generated for node Trusted accelerometer zone
Trustedaccelerometerzone_node1673322467440 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="default2",
        table_name="accelerometer_trusted",
        transformation_ctx="Trustedaccelerometerzone_node1673322467440",
    )
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = Join.apply(
    frame1=Trustedsteptrainerzone_node1,
    frame2=Trustedaccelerometerzone_node1673322467440,
    keys1=["sensorreadingtime"],
    keys2=["timestamp"],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Mean distance per SerialNumbre
MeandistanceperSerialNumbre_node1673323504096 = sparkAggregate(
    glueContext,
    parentFrame=ApplyMapping_node2,
    groups=["serialnumber"],
    aggs=[["distancefromobject", "avg"], ["x", "avg"], ["y", "avg"], ["z", "avg"]],
    transformation_ctx="MeandistanceperSerialNumbre_node1673323504096",
)

# Script generated for node Curated step-trainer zone
Curatedsteptrainerzone_node3 = glueContext.getSink(
    path="s3://cd0030bucket-daniel-avila/step_trainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="Curatedsteptrainerzone_node3",
)
Curatedsteptrainerzone_node3.setCatalogInfo(
    catalogDatabase="default2", catalogTableName="machine_learning_curated"
)
Curatedsteptrainerzone_node3.setFormat("json")
Curatedsteptrainerzone_node3.writeFrame(MeandistanceperSerialNumbre_node1673323504096)
job.commit()