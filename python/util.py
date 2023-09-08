import os
from config import Paths
import logging 
logging.basicConfig(level=logging.INFO)



def rawZoneSetup(paths):
    if(os.path.exists(paths.dataLake)== False):os.mkdir(paths.dataLake)
    if(os.path.exists(f"{paths.dataLake}/raw")== False):os.mkdir(f"{paths.dataLake}/raw")
    if(os.path.exists(paths.rawSearches)== False):os.mkdir(paths.rawSearches)
    if(os.path.exists(paths.rawVisitors)== False):os.mkdir(paths.rawVisitors)
    if(os.path.exists(paths.archive)== False):os.mkdir(paths.archive)
    if(os.path.exists(paths.archiveSearches)== False):os.mkdir(paths.archiveSearches)
    if(os.path.exists(paths.archiveVisitors)== False):os.mkdir(paths.archiveVisitors)

def getNewFiles(src:str,target:str):
    files=os.listdir(src)
    files=list(filter(lambda f:".json" in f, files))
    newFiles= list(filter(lambda f:fileExists(f,target)==False, files))
    logging.info(f"Total New Files {len(newFiles)}")
    return newFiles
    

def fileExists(filePath:str,target:str):
    ts=getTsFromFileName(filePath)
    if(os.path.exists(f"{target}/{ts}")==False):return False
    fName=getFileNameWithoutExtension(filePath)
    return os.path.exists(f"{target}/{ts}/{fName}.parquet")

def getFileNameWithoutExtension(fullPath:str)->str:
    return os.path.basename(fullPath).split(".")[0]

def getTsFromFileName(fullPath:str)->str:
    fName=os.path.basename(fullPath)
    return (fName.split("-")[-1]).split(".")[0]

def renameParquet(folderPath:str,newName:str):
    for f in os.listdir(folderPath):
        if(f.find("part")==0):
            logging.info(f"Rename {folderPath}/{f} to {folderPath}/{newName}")
            os.rename(f"{folderPath}/{f}",f"{folderPath}/{newName}")
            return 1

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
    # renameParquet(f"{paths.rawSearches}/20200126","xxx.parquet")
    # print(getNewFiles(paths.srcSearches,paths.rawSearches))
