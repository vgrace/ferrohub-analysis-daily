#!/usr/bin/env python3
import power_analysis_day
import base_load
import json
import re, time
import pymongo
import timeit
from timeit import default_timer as timer

import datetime_utilities
du = datetime_utilities

from debug_utilities import *

pad = power_analysis_day
base = base_load

is_debug = True
use_file = True

while True:
    cursor = pad.mdb_get_cursor()
    while cursor.alive:
        try:
            job_input = cursor.next()
            jobstart = timer()
            # New job found
            resultsid = job_input["resultsid"]
            debug_print(is_debug, use_file, ("DAILY Found job ",resultsid))
            job_input["starttime"]=du.round_down_datetime(job_input["starttime"])
            job_input["endtime"]=du.round_up_datetime(job_input["endtime"])
            debug_print(is_debug, use_file, job_input)
            # Get energy counter datafrom measurement DB
            aggr_data = pad.mdb_get_energy_counter_data_new(job_input) # pad.mdb_get_energy_counter_data_grouped(job_input)
            timer_counter_data = timer()
            # Fetch the base load values
            base_values = base.get_base_load_values(job_input)
            timer_base_values= timer()
            # Calculate the averages
            hub_aggr = pad.get_energy_counter_aggregate_new(aggr_data, base_values) # pad.get_energy_counter_aggregate(aggr_data, base_values)
            timer_aggregate= timer()
            job_input["data"]=list(hub_aggr)
            # Store the result in the local analysis database
            pad.mdb_insert_poweranalysisday_result(job_input)
            timer_aggregate= timer()
            job_results = {
                "energyhubid": job_input["energyhubid"],
                "starttime": job_input["starttime"] ,
                "endtime": job_input["endtime"],
                "userid": job_input["userid"],
                "resultsid":job_input["resultsid"],
                "analysismodel":job_input["analysismodel"],
                "jobstatus":1
            }
            pad.mdb_insert_poweranalysisday_jobs_results(job_results)
            # Mark the job done
            pad.mdb_mark_job_done(job_input)
            jobend = timer()
            debug_print(is_debug, use_file, ("DAILY Job ",resultsid," \nTotal: ", jobend - jobstart, "\nec data fetch: ",timer_counter_data - jobstart, "\base load data fetch: ",timer_base_values - timer_counter_data, "\aggregate: ", timer_aggregate - timer_base_values))
        except StopIteration:
            print("Out")
            time.sleep(1)
