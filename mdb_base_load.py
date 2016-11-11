#!/usr/bin/env python3
import analysis_config
import pymongo
from pymongo import CursorType
from datetime_utilities import *
from bson.son import SON 
connection = pymongo.MongoClient(analysis_config.main_mongodb_uri)
db = connection[analysis_config.main_mongodb]
local_connection = pymongo.MongoClient(analysis_config.local_mongodb_uri)
local_db = local_connection[analysis_config.local_mongodb]

def mdb_get_base_load_raw_data(device_mac, starttime, endtime):
    base_load_data = db[analysis_config.EHUBDATAS].find({"id" : device_mac, "$and" : [{"ts": { "$lte" : endtime}}, {"ts": { "$gte" : starttime}}]})
    return base_load_data

def mdb_get_base_load_energy_counter_data(device_mac, starttime, endtime):
    base_load_data = db[analysis_config.ENERGY_COUNTER].find({"id" : device_mac, "$and" : [{"ts": { "$lte" : endtime}}, {"ts": { "$gte" : starttime}}], "lcp1":{"$exists":True},"lcp2":{"$exists":True},"lcp3":{"$exists":True},"lcq1":{"$exists":True},"lcq2":{"$exists":True},"lcq3":{"$exists":True}}).sort("ts",pymongo.ASCENDING)
    return base_load_data

def mdb_get_base_load_calc(device_mac, starttime, endtime):
    base_load_data = local_db[analysis_config.BASE_LOAD_DAILY].find({"id" : device_mac, "$and" : [{"starttime": { "$lte" : endtime}}, {"starttime": { "$gte" : starttime}}]}).sort("ts",pymongo.ASCENDING)
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
    print("res",res)
    all_ids = db[analysis_config.ENERGY_COUNTER].distinct("id")
    print("all_ids",all_ids)
    start=datetime.now() - timedelta(days=30)
    for device_id in all_ids:
        if len([x for x in res if x["id"]==device_id])==0: 
            print("add",device_id)
            res.append({"id":device_id,"last_starttime":start,"starttime":start})
    return res

if __name__ == "__main__":
    # execute only if run as a script
    print("mdb_get_last_inserted()")
    print(mdb_get_last_inserted())
    print("mdb_get_base_load_calc()")
    print(list(mdb_get_base_load_calc("78:a5:04:ff:40:bb",datetime(2016,10,15,0,0,0),datetime(2016,10,17,0,0,0))))
    #print("mdb_get_base_load_energy_counter_data()")
    #print(list(mdb_get_base_load_energy_counter_data("78:a5:04:ff:40:bb",datetime(2016,10,15,0,0,0),datetime(2016,10,17,0,0,0))))
            