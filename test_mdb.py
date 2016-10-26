#!/usr/bin/env python3
from pymongo import MongoClient
import power_analysis_day
import json
from datetime import datetime
from datetime import timedelta

pad = power_analysis_day

def main():

    start_date = datetime.now() - timedelta(days=30)
    input = {
    "energyhubid": "78:a5:04:ff:40:bb",
    "starttime": start_date,
    "endtime": datetime.now() ,
    "userid": "testuser",
    "resultsid":"a1",
    "analysismodel":"POWERANALYSISDAY",
    }
    #print("\nRun the  aggregation for the input\n")
    #print(input)
    #print("\nGet aggregate for the input\n")
    aggr_data = pad.mdb_get_energy_counter_data_grouped(input)
    print(list(aggr_data))

connection = MongoClient("mongodb://CNET:CnET_mongo_1_!@172.31.26.143:27017/ehubdata?authMechanism=DEFAULT&authSource=ehubdata")
db = connection["ehubdata"]

print("-\n\n\n")
start_value = db.energydata.find_one( { "id" : "78:a5:04:ff:40:bb" })
print(start_value)
