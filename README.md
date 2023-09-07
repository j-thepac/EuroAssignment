- large-scale data we are retrieving from our web page and our mobile app via FTP in a **daily batch**.
- injects these data daily into a datalake in a proper structure.


 
partition date 
FTP in a daily batch.



## Task1:
------

###  Converting Source to Parq :
1. parq - compress , structure
use parquet, partition by date -> highly efficient column-wise compression, retains structure and type information, benefits - less storage more efficient in processing

compression="snappy": 
    - Snappy is known for its fast compression and decompression speeds, 
    Using compression can significantly reduce the storage space required for your data 
     improve the efficiency of data transfer and processing in distributed environments like Spark clusters.

### to avoid multiple triggers to mess up the data, 
- check if the partition of the date in the name of the file exists in datalake before writing
-  then quit the program if it does

### Other Alternatives :

- To use timestamp of the file created , can  be used as the save file name 
- This way we could handle any number of files combining 

Task2:
------
some preprocessing I can think of is
split visit_start and date_time columns into time and date columns
logged_in can be converted to boolean, hits_avg arounded to a standard decimal precision
replace nulls with some default

some dimensions to consider:
visitor(visitor id)
geography(country, region)
pages(pagename)

Task3:
------
group visitors dataset by visitor_id and visit_start_date, get max of visit_start_time, join back with visitor dataset inner join to get the latest entry of each visitor per day, you will need the visitor id, visit_start_date, country and region from this dataset

join with searches dataset on visitor id and visit_start_date = date in searches
then group by date, coutry and region to get the count of records in the new joined dataframe and thats the req output