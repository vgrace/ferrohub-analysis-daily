#!/usr/bin/env python3
import power_analysis_day
import json
from datetime import datetime
from datetime import timedelta

pad = power_analysis_day

def main():
 
    start_date = datetime.now() - timedelta(days=5)
    input = {
    "energyhubid": "78:a5:04:ff:40:bb",
    "starttime": start_date,
    "endtime": datetime.now() ,
    "userid": "testuser",
    "resultsid":"a1",
    "analysismodel":"POWERANALYSISDAY",
    }
    #input = {
    #"energyhubid": "78:a5:04:ff:40:bb",
    #"starttime": "2016-10-06T00:00:00.008Z",
    #"endtime": "2016-10-09T00:00:00.008Z",
    #"userid": "212",
    #"resultsid": "pouhiuguhg",
    #"analysismodel":"POWERANALYSISDAY"
    #}
    #print("\nRun the  aggregation for the input\n")
    #print(input)
    #print("\nGet aggregate for the input\n")
    aggr_data = pad.mdb_get_energy_counter_data_grouped(input)
    #print(aggr_data)
    #print("\nCalculate average and format\n")
    hub_aggr = pad.get_energy_counter_aggregate(aggr_data)
    input["data"]=list(hub_aggr)
    print(input)

if __name__ == "__main__":
    # execute only if run as a script
    main()
	
	







