#!/usr/bin/env python3
import power_analysis_day
import base_load
import json
import re, time
import pymongo
import datetime_utilities
du = datetime_utilities
pad = power_analysis_day
base = base_load

while True:
    cursor = pad.mdb_get_cursor()
    while cursor.alive:
        try:
            job_input = cursor.next()
            # New job found
            print("Found")
            job_input["starttime"]=du.round_down_datetime(job_input["starttime"])
            job_input["endtime"]=du.round_up_datetime(job_input["endtime"])
            print(job_input)
            # Get energy counter datafrom measurement DB
            aggr_data = pad.mdb_get_energy_counter_data_grouped(job_input)
            # Fetch the base load values
            base_values = base.get_base_load_values(job_input)
            # Calculate the averages
            hub_aggr = pad.get_energy_counter_aggregate(aggr_data, base_values)
            job_input["data"]=list(hub_aggr)
            # Store the result in the local analysis database
            pad.mdb_insert_poweranalysisday_result(job_input)
            # Mark the job done
            pad.mdb_mark_job_done(job_input)
        except StopIteration:
            print("Out")
            time.sleep(2)






            
