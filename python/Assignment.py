# %%
from pyspark.sql import SparkSession
from pyspark.sql.utils import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import StorageLevel
import util
from config import *
import logging
import os

# %%
logging.basicConfig(level=logging.INFO)
paths:Paths=Paths(
    dataLake="DataLake/"
    ,srcSearches="searches/"
    ,srcVisitors="visitors/"
    ,rawSearches="DataLake/raw/searches"
    ,rawVisitors="DataLake/raw/visitors"
    ,ezSearches="DataLake/ez/searches"
    ,ezVisitors="DataLake/ez/visitors"
    ,archive="archive/"
    ,archiveSearches="archive/searches"
    ,archiveVisitors="archive/visitors"
)
util.rawZoneSetup(paths)

# %% [markdown]
#                                                   Task 1: DATA INGESTION

# %%
spark=SparkSession\
.builder\
.appName("test")\
.getOrCreate()

spark.sparkContext.setCheckpointDir("sparkCache")

# %%
def dataIngestion(srcFolder,targetFolder,archiveFolder):
    for f in os.listdir(srcFolder):
        srcFile=f"{srcFolder}/{f}"
        if (".json" in f):
            ts=util.getTsFromFileName(f)
            targetPath=f"{targetFolder}/{ts}/"
            spark.read.json(f"{srcFolder}/{f}").coalesce(1).write.mode("append").options(header="True",compression="snappy").parquet(targetPath)
            os.rename(srcFile,f"{archiveFolder}/{f}")

dataIngestion(paths.srcSearches,paths.rawSearches,paths.archiveSearches)
dataIngestion(paths.srcVisitors,paths.rawVisitors,paths.archiveVisitors)

# %% [markdown]
#                                                       Task 2: PREPROCESSING

# %%
def cleanVisitor(df:DataFrame)->DataFrame:
    df=df\
    .withColumn("hits_avg",df["hits_avg"].cast(IntegerType()))\
    .withColumn("logged_in",df["logged_in"].cast(BooleanType()))\
    .withColumn("visit_start", udateHandler(df.visit_start) )\
    .withColumn("visits",df.visits.cast(IntegerType()))\
    .withColumn("visitor_id",trim(df.visitor_id.cast(StringType())))

    df=df.withColumn("visit_start", to_timestamp(df.visit_start, "yyyy-MM-dd HH:mm:ss"))
    df=df.withColumn("date", date_format(df.visit_start,"yyyy-MM-dd").cast(DateType()))\
    .na.fill("na",["visitor_id"])
    return df

def cleanSearches(df:DataFrame)->DataFrame:
    df= df\
    .withColumn("date_time",to_timestamp(df.date_time, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))\
    .withColumn("visitor_id",trim(df.visitor_id.cast(StringType())))
    
    df=df\
    .withColumn("date", date_format(df.date_time,"yyyy-MM-dd").cast(DateType()))\
    .na.fill("na",["visitor_id"])
    
    return df

def dateHandler(dateStr:str)->str:
    year=dateStr.split("-")[0]
    if(len(year)==2):
        dateStr="20"+dateStr
    return dateStr

udateHandler=udf(dateHandler)

# %%
rawVisitorDF= spark.read.options(header="True").options(inferSchema="True").parquet(f"{paths.rawVisitors}/*").cache()
logging.info(f" rawVisitorPartitions = {rawVisitorDF.rdd.getNumPartitions()} , rawVisitorDF.count={rawVisitorDF.count()} ")
cleanVisitorDF=rawVisitorDF.transform(cleanVisitor)
rawVisitorDF.unpersist()

rawSearchesDF= spark.read.options(header="True").options(inferSchema="True").parquet(f"{paths.rawSearches}/*").cache()
logging.info(f" rawSearchesPartitions={rawSearchesDF.rdd.getNumPartitions()} , rawSearchesPartitions ={rawSearchesDF.count()}")
cleanSearchesDF=rawSearchesDF.transform(cleanSearches)
rawSearchesDF.unpersist()

# %% [markdown]
#                                                   VISITOR DIMENSION

# %%
visitorDimension=cleanVisitorDF.select("visitor_id").distinct().withColumn("visitorkey",monotonically_increasing_id())
lastKey=visitorDimension.select(max(visitorDimension.visitorkey).alias("max")).first()["max"]

visitorDimension=cleanSearchesDF\
.select("visitor_id")\
.distinct()\
.join(visitorDimension,["visitor_id"],"left_anti")\
.select("visitor_id")\
.withColumn("visitorkey",monotonically_increasing_id()+lastKey+1)\
.union(visitorDimension)\
.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)

visitorDimension=visitorDimension\
.repartitionByRange(20,visitorDimension.visitorkey)\
.persist(storageLevel=StorageLevel.MEMORY_ONLY)

factVisitor=cleanVisitorDF.join(visitorDimension,["visitor_id"],"left_outer").persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
factSearches=cleanSearchesDF.join(visitorDimension,["visitor_id"],"left_outer").persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
factVisitor.checkpoint()
factSearches.checkpoint()

# %% [markdown]
#                                                   PERIOD DIMENSION (Date)

# %%
peridDimension=cleanVisitorDF.select("date").distinct().withColumn("datekey",monotonically_increasing_id())
maxPeriodKey=peridDimension.select(max(peridDimension.datekey).alias("max")).first()["max"]

peridDimension=cleanSearchesDF\
.select("date")\
.distinct()\
.join(peridDimension,["date"],"left_anti")\
.select("date")\
.distinct()\
.withColumn("datekey",maxPeriodKey+monotonically_increasing_id()+1)\
.union(peridDimension)

peridDimension=peridDimension\
.repartitionByRange(20,peridDimension.datekey)\
.persist(storageLevel=StorageLevel.MEMORY_ONLY)

factVisitor=factVisitor.join(peridDimension,["date"],"left_outer").persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
factSearches=factSearches.join(peridDimension,["date"],"left_outer").persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
factVisitor.checkpoint()
factSearches.checkpoint()

# %%
#                                               Verify Count source and intermitten result
assert(rawVisitorDF.count() == factVisitor.count() )
assert(rawSearchesDF.count() == factSearches.count())


# %% [markdown]
#                                                       Task3: REPORTS

# %%
# With Period Dimesion
factVisitorGrouped=factVisitor.groupBy("visitorkey",factVisitor.datekey).agg(max("visit_start").alias("visit_start")).cache()
factVisitorExtended=factVisitorGrouped\
                        .join(factVisitor,["visitorkey","visit_start"])\
                        .select("visitorkey","visit_start",factVisitorGrouped.datekey,"country","region")\
                        .withColumnRenamed("visit_start","date_time")

# %% [markdown]
#                                                   FINAL RESULT

# %%
## Without Period Dim
# result = factSearches\
# .join(factVisitorExtended,["visitorkey","date"],"left_outer")\
# .select("country","region","visitorkey","date")\
# .groupBy("date","country","region")\
# .agg(count("*").alias("count")).cache()

## With Period Dim
result = factSearches\
.join(factVisitorExtended,["visitorkey","datekey"],"left_outer")\
.select("country","region","visitorkey",factSearches.date)\
.groupBy("date","country","region")\
.agg(count("*").alias("count")).cache()

# %%
result.coalesce(1).write.options(header="True").csv("Result")
sampleDate="(cast('2021-03-05' as date),cast('2021-04-12' as date),cast('2021-01-27' as date),cast('2021-05-02' as date),cast('2021-05-08' as date) )"
result.filter(f"date in {sampleDate}").show()

# %%
# country and region not Found  - but should be esp , co
# cleanVisitorDF.filter(f"date = cast('2021-04-12' as date)").show()
exit(0)
