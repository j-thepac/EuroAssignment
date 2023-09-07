import os
from config import Paths
import logging 
logging.basicConfig(level=logging.INFO)



def rawZoneSetup(paths):
    if(os.path.exists(paths.dataLake)== False):os.mkdir(paths.dataLake)
    if(os.path.exists(f"{paths.dataLake}/raw")== False):os.mkdir(f"{paths.dataLake}/raw")
    if(os.path.exists(paths.rawSearches)== False):os.mkdir(paths.rawSearches)
    if(os.path.exists(paths.rawVisitors)== False):os.mkdir(paths.rawVisitors)

def getNewFiles(src:str,target:str):
    files=os.listdir(src)
    newFiles=list(filter ( lambda f: os.path.exists( f"{target}/{getTsFromFileName(f)}") ==False ,files ))
    logging.info(f"getNewFiles(): path {src}, new Files Count {len(newFiles)}")
    return newFiles
 

def getTsFromFileName(fullPath:str)->str:
    fName=os.path.basename(fullPath)
    return (fName.split("-")[-1]).split(".")[0]


if(__name__ == "__main__"):

    paths=Paths(
        dataLake="DataLake/"
        ,srcSearches="searches/"
        ,srcVisitors="visitors/"
        ,rawSearches="DataLake/raw/searches"
        ,rawVisitors="DataLake/raw/visitors"
        ,ezSearches="DataLake/ez/searches/"
        ,ezVisitors="DataLake/ez/visitors/"
        )

    getNewFiles(paths.srcSearches,paths.rawSearches)
