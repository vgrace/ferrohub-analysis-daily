#!/usr/bin/env python3
import power_analytics_day
import json
from datetime import datetime
from datetime import timedelta

pad = power_analytics_day

print("am_batch_power_analytics_day\n")

print("\nmdb_get_energy_counter_enabled_hubs")
enabled_hubs = pad.mdb_get_energy_counter_enabled_hubs()
print(enabled_hubs)
print("\nmdb_get_last_inserted")
last_insert_dates = pad.mdb_get_last_inserted("daily")
print(last_insert_dates)	

print("\nRun the aggregation one hub at a time\n")
# Run the aggregation one hub at a time
for ehub in enabled_hubs:
    start_date = next((x["last_ts"] for x in last_insert_dates if x["_id"]["id"]==enabled_hubs[0]["_id"]["id"]), None)
    if start_date == None:
        start_date = datetime.now() - timedelta(years=1)
    input = {
    "energyhubid": ehub["_id"]["id"],
    "starttime": start_date + timedelta(days=1),
    "endtime": datetime.now() ,
    "userid": "string"
    }
    hub_aggr = pad.get_energy_counter_aggregate(pad.mdb_get_energy_counter_data_grouped(input))
    docs = list(hub_aggr)
    print(docs)
    pad.mdb_insert_power_aggregates(docs)







