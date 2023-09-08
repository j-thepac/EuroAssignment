
from dataclasses import dataclass

@dataclass
class Paths:
    dataLake:str
    srcSearches:str
    srcVisitors:str
    rawSearches:str
    rawVisitors:str
    ezSearches:str
    ezVisitors:str
    archive:str
    archiveSearches:str
    archiveVisitors:str