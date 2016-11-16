#!/usr/bin/env python3
import analysis_config
import pymongo
from pymongo import CursorType
from datetime import datetime
# from datetime import date
# from datetime import timedelta
# from datetime import time
from datetime_utilities import *
## New imports
import collections
import time
import math
from bson.son import SON # As python dictionaries don’t maintain order you should use SON or collections.OrderedDict where explicit ordering is required eg “$sort”:

connection = pymongo.MongoClient(analysis_config.main_mongodb_uri)
db = connection[analysis_config.main_mongodb]
local_connection = pymongo.MongoClient(analysis_config.local_mongodb_uri)
local_db = local_connection[analysis_config.local_mongodb]
## New
timezone_offset_ms = int(math.fabs(time.timezone * 1000))
result_object = collections.namedtuple('Result', ['index', 'res'])

## DATABASE ##

# POWERANALYSISDAILY_JOBS -> nullanalysis_jobs
def mdb_get_cursor():
    cur = local_db["nullanalysis_jobs"].find(filter={'jobstatus' : {"$eq":0}},cursor_type=CursorType.TAILABLE_AWAIT)
    #cur = cur.hint([('$natural', 1)]) # ensure we don't use any indexes
    return cur

# POWERANALYSISDAILY_RESULTS -> nullanalysis_results
def mdb_insert_poweranalysisday_result(resultdata):
    #resultdata["_id"]=None
    local_db["nullanalysis_results"].insert(resultdata)

# POWERANALYIS_DAILY_JOBS_RESULTS -> nullanalysis_jobs_results
def mdb_setup_poweranalysisday_jobs_results_collection():
    local_db.create_collection('nullanalysis_jobs_results', capped= True, size= 65536, autoIndexId = False )
    input = {
    "energyhubid": "00:00:00:00:00:00",
    "starttime": datetime.now() ,
    "endtime": datetime.now()+timedelta(days=1) ,
    "userid": "testuser",
    "resultsid":"testresultsid",
    "analysismodel":"DAILYPOWER",
    "jobstatus":1
    }
    mdb_insert_poweranalysisday_jobs_results(input)

def mdb_insert_poweranalysisday_jobs_results(jobdata):
    local_db["nullanalysis_jobs_results"].insert(jobdata)

def mdb_insert_poweranalysisday_job(jobdata):
    local_db["nullanalysis_jobs"].insert(jobdata)

def mdb_setup_poweranalysisday_job_collection():
    local_db.create_collection('nullanalysis_jobs', capped= True, size= 65536, autoIndexId = False )
    input = {
    "energyhubid": "00:00:00:00:00:00",
    "starttime": datetime.now() ,
    "endtime": datetime.now()+timedelta(days=1) ,
    "userid": "testuser",
    "resultsid":"testresultsid",
    "analysismodel":"HOURLYPOWER",
    "jobstatus":0
    }
    mdb_insert_poweranalysisday_job(input)

def mdb_mark_job_done(jobdata):
    print(jobdata["resultsid"])
    local_db["nullanalysis_jobs"].find_and_modify(query={'resultsid':jobdata["resultsid"]}, update={"$set": {'jobstatus': 1}}, upsert=False, full_response= False)

## ANALYSIS ##
def mdb_get_energy_counter_data_new(input):
    resultat = {} #[]
    resultat["ts"] = datetime.now().timestamp()
    #ts = datetime.now()
    #resultat.insert(0, ts)
    
    return resultat


def get_energy_counter_aggregate_new(last_list, base_load_values):
    aggr_res = []
    aggr_res.insert(0, last_list)
    return aggr_res

