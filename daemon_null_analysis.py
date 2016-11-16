#!/usr/bin/env python3
#import power_analysis_day
import null_analysis
#import base_load
import json
import re, time
#import pymongo

import datetime_utilities
du = datetime_utilities

import timeit
from timeit import default_timer as timer

from debug_utilities import *
is_debug = True
use_file = True

pad = null_analysis #power_analysis_day
#base = base_load

while True:
    cursor = pad.mdb_get_cursor()
    while cursor.alive:
        try:
            job_input = cursor.next()
            jobstart = timer()
            # New job found
            resultsid = job_input["resultsid"]
            debug_print(is_debug, use_file, ("\nNULL Found job ",resultsid))
            job_input["starttime"]=du.round_down_datetime(job_input["starttime"])
            job_input["endtime"]=du.round_up_datetime(job_input["endtime"])
            debug_print(is_debug, use_file, job_input)
            # Get energy counter datafrom measurement DB
            aggr_data = pad.mdb_get_energy_counter_data_new(job_input) # Returns timestamp
            # Fetch the base load values
            base_values = [] #base.get_base_load_values(job_input)
            # Calculate the averages
            hub_aggr = pad.get_energy_counter_aggregate_new(aggr_data, base_values)
            job_input["data"]=list(hub_aggr)
            # Store the result in the local analysis database
            pad.mdb_insert_poweranalysisday_result(job_input)
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
            debug_print(is_debug, use_file, ("\nNULL Job ",resultsid," ", jobend - jobstart))
        except StopIteration:
            print("Out")
            time.sleep(0.5)