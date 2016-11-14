#!/usr/bin/env python3
import power_analysis_day
import base_load
import json
from datetime import datetime
from datetime import timedelta
from datetime_utilities import *
import time
import timeit
from timeit import default_timer as timer



base = base_load
pad = power_analysis_day

def main():
 
    start_date = datetime.now() - timedelta(days=30)
    input = {
    "energyhubid": "78:a5:04:ff:40:bb", # •fe-ace-28 Örebrobostäder Granrisvägen     78:a5:04:ff:40:bb
    "starttime": datetime(2016,11,7,0,0,0),
    "endtime": datetime(2016,11,10,0,0,0),
    "userid": "testuser",
    "resultsid":"a1",
    "analysismodel":"POWERANALYSISDAY",
    }
    print("\nRun the  aggregation for the input\n")
    #print(input)
    input["starttime"]=round_down_datetime(input["starttime"])
    input["endtime"]=round_up_datetime(input["endtime"])
    print("\nGet aggregate for the input\n")
    print(input)
    # Get energy counter datafrom measurement DB
    aggr_data = pad.mdb_get_energy_counter_data_grouped(input)
    print("\mdb_get_energy_counter_data_grouped(input)\n")
    print(aggr_data)
    # Fetch the base load values
    base_values = base.get_base_load_values(input)
    print("\get_base_load_values(input)\n")
    print(list(base_values))
    # Calculate the averages
    hub_aggr = pad.get_energy_counter_aggregate(aggr_data, base_values)
    print("\get_base_load_values(input)\n")
    print(hub_aggr)
    input["data"]=list(hub_aggr)
    #print(input)
    # Store the result in the local analysis database
    #pad.mdb_insert_poweranalysisday_result(input)
    # Mark the job done
    #pad.mdb_mark_job_done(input)
    # print(sorted(hub_aggr_list, key=lambda x: x["ts"]))   # sort by ts


if __name__ == "__main__":
    # execute only if run as a script
    main()
	
	






