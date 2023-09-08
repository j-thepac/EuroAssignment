# Data Engineering Challenge

## Goal

- code which injects data **data daily**
- In **proper structure**.

## Framework : Pyspark
Used  to ingest the Data for below advantages
-  MultiCore InMemory Parallel Processing Capablity .
- Fault Tolerance 
- High Level API and SQL 
- Ease of Use 


## Folder Structure 
- DataLake : Store Data post processing
    - raw : Extract - Load as parquet
    - ez (future ) : To Store Cleaned Data / Transformed Data / Build Data Warehouse 

- searches: Input files for searches are dropped here 
- visitors: Input files for visitors are dropped here 
- archive : Once Data is processed , input files are archived here
    - searches: Input files from searches are moved here
    - visitors: Input files from visitors are moved here
- sparkCache: Store Spark Cache Data

--------------------
## Task 1: Data Ingestion
1. Read all files that are given in this assignment 
2. Based on the timestamp of each file's name
3. Ingest into the datalake
4. Pipeline could be accidentally triggered multiple times in a day

### Solution Implemented :
I have implemented a logic mechanism to relocate JSON files to an archive folder upon completion of processing. This precautionary measure is designed to mitigate potential issues that may arise from unintentional execution of the pipeline.

1. With the above logic the data is inherently saved with timestamps and inherently partitioned. Consequently, this minimizes data skewness while further processing in spark
2. Following the ingestion of input files, the files archived, there is no risk of false triggers or unintended modifications.
3. The archived data serves as a reliable backup and can be retained or purged as required, aligning with specific operational needs.
4. There is no need to have additional overhead like WaterMarking / Code Complexity to overcome this challenge.
5. Above logic is Simple and Reliable.

#### Format Used : parquet
1. Retaining the data structure and type information is a key advantage of this approach.
2. This strategy offers multiple benefits, including reduced storage requirements and improved processing efficiency.
3. When setting the compression "snappy," which offers rapid compression and decompression capabilities.
4. Utilizing compression can result in a significant reduction in the storage space needed for your data, 
5. ultimately enhancing data transfer and processing efficiency in distributed environments like Spark clusters.

### Example:
For input 

    visitor-part-01-2020123.json
    visitor-part-02-2020123.json
Are Stored in same Folder 

    Datalake/raw/2020123/xxx1.parq
    Datalake/raw/2020123/xxx2.parq

--------------------

## Task 2: Preprocessing

1. Consume Data from Lake
2. changes you will perform,
3. why 
4. which format and structure 
5. optimize and increase the performance. 

### Solution Implemented 
1. Given that data is partitioned for consumption, it should be evenly distributed among each Spark Executor.
2. In order to minimize multiple reads, the data is cached during the initial reading process, and subsequently cleared after cleansing.
3. I have thoroughly reviewed the requirements and the requested data.
4. As per the specifications, we require the following fields from the visitor table: visitor_id, date, region, and country.
5. From the searches table, we need date and visitor_id.
6. Among the available columns, visitor_id and date_time hold the utmost significance.
7. Furthermore, "visitor_id" may be regarded as a dimension.
     - (Assumption) Typically, visitor_id is auto-generated, maintains uniqueness, and remains static.
     - It is a common attribute shared by both tables.
     - It serves as a key for performing joins.
     - Generally, visitor_id is finite in nature.
8. Considering the substantial size of our dataset, it is imperative to persist intermediate results for optimal performance.
9. Since our data can be quite extensive, caching it in memory might lead to overflow issues. Therefore, implementing a Checkpoint Direct approach is advisable.

#### **date_time**
1. Standardize the format in both tables to ensure consistency.
2. Generate a  date column, exclusively containing "date" values, derived from the date_time attribute.
3. [Future] This may potentially qualify as a candidate for inclusion as a Dimension, particularly as a Time Period Dimension, in subsequent developments.

#### **visitor_id ( Dimension )**
1. Convert the data to a string format and subsequently apply a trimming process.
2. Address and rectify any occurrences of null values within the dataset.
3. Create a series of natural numbers that serve as unique keys for each distinct visitor_id.
4. Employ these unique keys as the primary means of joining datasets, as this optimization streamlines subsequent operations.
5. It is advisable to cache this dataset for the purpose of enhancing operational efficiency in future processes.
------------------------------
## Task3: Reports

Utilizing the key generated in the preceding step, the two datasets are merged and subsequently grouped in order to derive the specified results.By employing the dimension table, end users can attain the desired outcomes.


------------------------------
## Task4: Pipeline architecture
-------
1. multiple large datasets every 10 minutes, 
2. how do you automatize this task
3. Which tools
4. which strategy would you use? 
    -  a simple architecture
    - infrastructure of your desired system.


### Design 1 
#### Tools :Cron Job
1. To implement a cron jon to spool very 10 minsutes on the folders in consideration
2. Keep a long running Spark Cluster to ingest the Data from Source to DateLake 

### Design 2
#### Tools : Orechestartion Tool , AirFlow DAG
1. To keep it seperate we can have a Orechestartion tool like Jenkins / Travis to veirfy if there is any new files coming in the folder which would inturn would trigger trigger
2. 



```mermaid
  graph TD;
      A-->B;
      A-->C;
      B-->D;
      C-->D;
```
Assuming you get multiple large datasets every 10 minutes, 
- Cron Job
- Jenkins / Travis 

how do you automatize this task? 
- AirFlow Dag with a Cron

- Azure Pipeline 

Which tools, 
which strategy would you use? 
Please give us only a simple architecture, including the infrastructure of your desired system.


- Cron Job
- 