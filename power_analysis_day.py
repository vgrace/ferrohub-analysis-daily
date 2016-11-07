#!/usr/bin/env python3
import analysis_config
import pymongo
from pymongo import CursorType
from datetime import datetime
# from datetime import date
# from datetime import timedelta
# from datetime import time
from datetime_utilities import *
from bson.son import SON # As python dictionaries don’t maintain order you should use SON or collections.OrderedDict where explicit ordering is required eg “$sort”:

connection = pymongo.MongoClient(analysis_config.main_mongodb_uri)
db = connection[analysis_config.main_mongodb]
local_connection = pymongo.MongoClient(analysis_config.local_mongodb_uri)
local_db = local_connection[analysis_config.local_mongodb]

def mdb_get_energy_counter_data(device_mac, date):
    end_date =  date + timedelta(days=1)
    start_value = db.energydata.find_one( { "id" : device_mac, "ts": { "$gte" : date}})
    end_value = db.energydata.find_one( { "id" : device_mac, "ts": { "$lte" : end_date}})
    return start_value, end_value

def mdb_setup_poweranalysisday_job_collection():
    local_db.create_collection(analysis_config.POWERANALYSISDAILY_JOBS, capped= True, size= 65536, autoIndexId = False )
    input = {
    "energyhubid": "00:00:00:00:00:00",
    "starttime": datetime.now() ,
    "endtime": datetime.now()+timedelta(days=1) ,
    "userid": "testuser",
    "resultsid":"testresultsid",
    "analysismodel":"POWERANALYSISDAY",
    "jobstatus":0
    }
    mdb_insert_poweranalysisday_job(input)

def mdb_insert_poweranalysisday_jobs_results(jobdata):
    local_db[analysis_config.POWERANALYIS_DAILY_JOBS_RESULTS].insert(jobdata)


def mdb_insert_poweranalysisday_job(jobdata):
    local_db[analysis_config.POWERANALYSISDAILY_JOBS].insert(jobdata)

def mdb_mark_job_done(jobdata):
    print(jobdata["resultsid"])
    local_db[analysis_config.POWERANALYSISDAILY_JOBS].find_and_modify(query={'resultsid':jobdata["resultsid"]}, update={"$set": {'jobstatus': 1}}, upsert=False, full_response= False)
	
def mdb_insert_poweranalysisday_result(resultdata):
    #resultdata["_id"]=None
    local_db[analysis_config.POWERANALYSISDAILY_RESULTS].insert(resultdata)

def mdb_get_cursor():
    cur = local_db[analysis_config.POWERANALYSISDAILY_JOBS].find(filter={'jobstatus' : {"$eq":0}},cursor_type=CursorType.TAILABLE_AWAIT)
    #cur = cur.hint([('$natural', 1)]) # ensure we don't use any indexes
    return cur

def mdb_get_last_inserted(period):
    res = list(db["power_"+period].aggregate(pipeline=
        [
         { "$sort": { "ts": 1} },
        {"$group" :{
        "_id": {"id":"$id", "fid":"$fid"},
        "last_ts": {"$last":"$ts"}
        }}]))
    return res

def mdb_get_energy_counter_enabled_hubs():
    res = list(db[analysis_config.ENERGY_COUNTER].aggregate(pipeline=
        [
        {"$group" :{
        "_id": {"id":"$id", "fid":"$fid"}
        }}]))
    return res

def mdb_insert_power_aggregates(power_aggregate_list):
    db["testar_aggr"].insert_many(power_aggregate_list)

def mdb_get_energy_counter_data_grouped(input):
    """
        Fetches energy counter data for an ehub between two dates, returning first and last values for each day.
        If necessary, $project + $subtract + $divide could be used to calculate the averages in MDB.
        Another variation that does not require sorting is to use $max, $min instead of $last, $first
		
        Input:
        {
         "energyhubid": string, 
         "starttime": datetime ,
         "endtime": datetime,
         "userid": "string,
         "resultsid": string,
         "analysismodel": string, # "POWERANALYSISDAY", "POWERANALYSISHOUR" 
         "jobstatus": int # 0 = created, 1 = result ready
         }
		 
		 The output has 
		 "first_ts"
		 "last_ts"
		 adjusted to EHUB time (CEST).
        """
    starttime_as_utc = cest_as_utc(round_down_datetime(input["starttime"]))
    endtime_as_utc =  cest_as_utc(round_up_datetime(input["endtime"]))
    res = list(db[analysis_config.ENERGY_COUNTER].aggregate(pipeline=
        # Select from an cest-adjusted time to a cest-adjusted time (i.e., if starttime CEST is 2016-10-10T00:00:00, we should select from UTC 2016-10-09T22:00:00 to get the first two hours of 2016-10-10.)
        [{"$match" :{"id": input["energyhubid"] , "ts":{"$gte": starttime_as_utc, "$lte": endtime_as_utc}}},
        { "$sort": { "ts": 1} }, # order by ascending date
        {"$group" :{
        # When grouping, use the same offset as for $match, the datetime found in db is UTC, so we add 2 hours to that to group by CEST days
        "_id": {"device_mac":"$id", "fid":"$fid", "year":{"$year":{"$add" : ["$ts",  cest_offset_ms]}},"month":{"$month":{"$add" : ["$ts",  cest_offset_ms]}},"day":{"$dayOfMonth":{"$add" : ["$ts",  cest_offset_ms]}}},
    "first_ts": {"$first":{"$add" : ["$ts",  cest_offset_ms]}}, # We want this  timestamp to be expressed in EHUB time zone (CEST)
    "last_ts": {"$last":{"$add" : ["$ts",  cest_offset_ms]}}, # We want this timestamp to be expressed in EHUB time zone (CEST)
    # Energy External Production positive direction
    "first_epq1": {"$first":"$epq1"},
    "last_epq1": {"$last":"$epq1"},
    "first_epq2": {"$first":"$epq2"},
    "last_epq2": {"$last":"$epq2"},
    "first_epq3": {"$first":"$epq3"},
    "last_epq3": {"$last":"$epq3"},
    # Energy External Consumption negative direction
    "first_ecq1": {"$first":"$ecq1"},
    "last_ecq1": {"$last":"$ecq1"},
    "first_ecq2": {"$first":"$ecq2"},
    "last_ecq2": {"$last":"$ecq2"},
    "first_ecq3": {"$first":"$ecq3"},
    "last_ecq3": {"$last":"$ecq3"},
    # Energy Internal Production positive direction
    "first_ipq1": {"$first":"$ipq1"},
    "last_ipq1": {"$last":"$ipq1"},
    "first_ipq2": {"$first":"$ipq2"},
    "last_ipq2": {"$last":"$ipq2"},
    "first_ipq3": {"$first":"$ipq3"},
    "last_ipq3": {"$last":"$ipq3"},
    # Energy Internal Consumption positive direction
    "first_icq1": {"$first":"$icq1"},
    "last_icq1": {"$last":"$icq1"},
    "first_icq2": {"$first":"$icq2"},
    "last_icq2": {"$last":"$icq2"},
    "first_icq3": {"$first":"$icq3"},
    "last_icq3": {"$last":"$icq3"},
    # Energy Load Production positive direction
    "first_lcp1": {"$first":"$lcp1"},
    "last_lcp1": {"$last":"$lcp1"},
    "first_lcp2": {"$first":"$lcp2"},
    "last_lcp2": {"$last":"$lcp2"},
    "first_lcp3": {"$first":"$lcp3"},
    "last_lcp3": {"$last":"$lcp3"},
    # Energy Load Consumption negative direction
    "first_lcq1": {"$first":"$lcq1"},
    "last_lcq1": {"$last":"$lcq1"},
    "first_lcq2": {"$first":"$lcq2"},
    "last_lcq2": {"$last":"$lcq2"},
    "first_lcq3": {"$first":"$lcq3"},
    "last_lcq3": {"$last":"$lcq3"},
    # PV energy
    "first_pve": {"$first":"$pve"},
    "last_pve": {"$last":"$pve"},
    # Energy battery Production positive direction
    "first_bp": {"$first":"$bp"},
    "last_bp": {"$last":"$bp"},
    # Energy battery consumption = this is charging battery - negative direction
    "first_bc": {"$first":"$bc"},
    "last_bc": {"$last":"$bc"}
        }
        }], allowDiskUse=True))
    return res

def unsigned64int_from_words(high, low, signed):
    """returns an unsigned int64 from high, low 4-byte values."""
    return int.from_bytes((high).to_bytes(4,byteorder='big',signed=True)+(low).to_bytes(4,byteorder='big', signed=True),byteorder='big',signed=False)

# Use for instead of map if we do not need or want to create a new list
def get_energy_counter_aggregate(first_last_list, base_load_values):
    """Calculates  the average and base values for fetched energy counters aggregate (first_last_list)."""
    # All this iterating over lists should be replaced with numpy array broadcast(slicing)
    map_args = []
    for first_last in first_last_list:
        map_args.append({"avg":first_last, "base":base_load_values})
    return map(get_energy_counter_averages, map_args)

def get_energy_counter_averages(aggregate_values_and_base_loads):
    """Calculate the averages and return outdata (not trivial to do in db query)"""
    periodvalues = {}
    energy_counter_data = {}
    ts = aggregate_values_and_base_loads["avg"]["first_ts"]
    adjusted_ts = round_down_datetime(ts) # The first value is in EHUB time (CEST) but may not be rounded down.
    print("aggr-base start",adjusted_ts,"-",aggregate_values_and_base_loads["base"]))
    periodvalues["ts"]=adjusted_ts.timestamp() # This value is already adjusted to EHUB time (CEST)
    for avg_name in ["epq1","epq2","epq3","ecq1","ecq2","ecq3","ipq1","ipq2","ipq3","icq1","icq2","icq3","lcp1","lcp2","lcp3","lcq1","lcq2","lcq3","pve","bp","bc"]:
        first_value = aggregate_values_and_base_loads["avg"]["first_"+avg_name]
        last_value = aggregate_values_and_base_loads["avg"]["last_"+avg_name]
        if (first_value != None and last_value != None):
            # energy conunter values are in mJ, convert them to kWh
            first_value_64 = unsigned64int_from_words(first_value[0],first_value[1], not(first_value[2]))/3600000000
            last_value_64 = unsigned64int_from_words(last_value[0],last_value[1], not(last_value[2]))/3600000000
            energy_counter_data[avg_name]=(last_value_64-first_value_64)/24
        else:
            energy_counter_data[avg_name]=0
    # Set up return values (in kW)
    periodvalues["aipL1"]=energy_counter_data["lcp1"]
    periodvalues["aipL2"]=energy_counter_data["lcp2"]
    periodvalues["aipL3"]=energy_counter_data["lcp3"]
    periodvalues["aip"] = periodvalues["aipL1"] + periodvalues["aipL2"] + periodvalues["aipL3"]
    # Reactive power values are currently not present in energy counter data
    periodvalues["rip"]=None
    periodvalues["ripL1"]=None
    periodvalues["ripL2"]=None
    periodvalues["ripL3"]=None
    # Base power may be fetched from DB elsewhere but we set them up here for now
    # The timestamps in base_loads should be rounded starttimes for the selected time span, in EHUB time (CEST)
    # print(aggregate_values_and_base_loads["base"])
    base_loads = next(filter(lambda x: x["starttime"]==adjusted_ts, aggregate_values_and_base_loads["base"]), None)
    if base_loads != None:	
        periodvalues["abp"]=base_loads["abp"]
        periodvalues["abpL1"]=base_loads["abpL1"]
        periodvalues["abpL2"]=base_loads["abpL2"]
        periodvalues["abpL3"]= base_loads["abpL3"]
        periodvalues["rbp"]=base_loads["rbp"]
        periodvalues["rbpL1"]=base_loads["rbpL1"]
        periodvalues["rbpL2"]=base_loads["rbpL2"]
        periodvalues["rbpL3"]=base_loads["rbpL3"]
    else:
        periodvalues["abp"]=None
        periodvalues["abpL1"]=None
        periodvalues["abpL2"]=None
        periodvalues["abpL3"]= None
        periodvalues["rbp"]=None
        periodvalues["rbpL1"]=None
        periodvalues["rbpL2"]=None
        periodvalues["rbpL3"]=None
    return periodvalues
