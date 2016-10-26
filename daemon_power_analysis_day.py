#!/usr/bin/env python3
import power_analysis_day
import json
import re, time
import pymongo
pad = power_analysis_day
while True:
    cursor = pad.mdb_get_cursor()
    while cursor.alive:
        try:
            doc = cursor.next()
            print("Found")
            print(doc)
            # New job found
            aggr_data = pad.mdb_get_energy_counter_data_grouped(doc)
            hub_aggr = pad.get_energy_counter_aggregate(aggr_data)
            doc["data"]=list(hub_aggr)
            pad.mdb_insert_poweranalysisday_result(doc)
            pad.mdb_mark_job_done(doc)
        # do_something(msg)
        except StopIteration:
            #print("Out")
            time.sleep(0.2)
