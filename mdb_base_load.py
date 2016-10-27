#!/usr/bin/env python3
import analysis_config
import pymongo
from pymongo import CursorType
# from datetime import datetime
# from datetime import date
# from datetime import timedelta
# from datetime import time
from datetime_utilities import *
from bson.son import SON # As python dictionaries don’t maintain order you should use SON or collections.OrderedDict where explicit ordering is required eg “$sort”:
connection = pymongo.MongoClient(analysis_config.main_mongodb_uri)
db = connection[analysis_config.main_mongodb]
local_connection = pymongo.MongoClient(analysis_config.local_mongodb_uri)
local_db = local_connection[analysis_config.local_mongodb]

def mdb_get_base_load_raw_data(device_mac, starttime, endtime):
    base_load_data = db[analysis_config.EHUBDATAS].find({"id" : device_mac, "$and" : [{"ts": { "$lte" : endtime}}, {"ts": { "$gte" : starttime}}]})
    return base_load_data

def mdb_get_base_load_calc(device_mac, starttime, endtime):
    base_load_data = local_db[analysis_config.BASE_LOAD_DAILY].find({"id" : device_mac, "$and" : [{"starttime": { "$lte" : endtime}}, {"starttime": { "$gte" : starttime}}]})
    return base_load_data
	
def mdb_insert_base_load_calc(base_load_for_period):
    local_db[analysis_config.BASE_LOAD_DAILY].insert(base_load_for_period)

def mdb_get_last_inserted():
    res = list(local_db[analysis_config.BASE_LOAD_DAILY].aggregate(pipeline=
        [
         { "$sort": { "starttime": 1} },
        {"$group" :{
        "_id": {"id":"$id"},
        "id": {"$last":"$id"},
		"last_starttime": {"$last":"$starttime"}
        }}]))
    return res	

if __name__ == "__main__":
    # execute only if run as a script
    print("Get precalculated base_load")
    print(list(mdb_get_base_load_calc("78:a5:04:ff:40:bb", datetime(2016,10,5,0,0,0), datetime(2016,10,15,0,0,0))))
    print("Get mdb_get_last_inserted")
    last = mdb_get_last_inserted()
    print(last)
    for device in last:
        print(device["id"], device["last_starttime"], datetime.today().date())
        if device["last_starttime"] < round_down_datetime(datetime.today()):
            raw = mdb_get_base_load_raw_data(device["id"], device["last_starttime"], round_down_datetime(datetime.today()))
            print(raw.count())
            
		