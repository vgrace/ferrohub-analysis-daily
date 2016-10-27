#!/usr/bin/env python3
import analysis_config
import pymongo
from datetime import datetime
from datetime import timedelta
from random import randint

import power_analysis_day
import mdb_base_load

base = mdb_base_load
pad = power_analysis_day

connection = pymongo.MongoClient(analysis_config.main_mongodb_uri)
db = connection[analysis_config.main_mongodb]
local_connection = pymongo.MongoClient(analysis_config.local_mongodb_uri)
local_db = local_connection[analysis_config.local_mongodb]


# start_date = "2016-09-30"
# stop_date = "2016-10-31"
# start = datetime.strptime(start_date, "%Y-%m-%d")
# stop = datetime.strptime(stop_date, "%Y-%m-%d")
# high = randint(0,77)
# low = randint(-439459679,1827293614)
# increment = 0
# while start < stop:
    # result= db[analysis_config.ENERGY_COUNTER].insert_one(
        # {
        # "ts" : start,
        # "id" : "78:a5:04:ff:40:bb",
        # "fid" : 1337,
        # "epq1" : [
                # high + increment,
                # low + increment,
                # True
        # ],
        # "epq2" : [
                # high + increment,
                # low + increment,
                # True
        # ],
        # "epq3" : [
                # high + increment,
                # low + increment,
                # True
        # ],
        # "ecq1" : [
                # high + increment,
                # low + increment,
                # True
        # ],
        # "ecq2" : [
                # high + increment,
                # low + increment,
                # True
        # ],
        # "ecq3" : [
                # high + increment,
                # low + increment,
                # True
        # ],
        # "ipq1" : [
                # high + increment,
                # low + increment,
                # True
        # ],
        # "ipq2" : [
                # high + increment,
                # low + increment,
                # True
        # ],
        # "ipq3" : [
                # high + increment,
                # low + increment,
                # True
        # ],
        # "icq1" : [
                # high + increment,
                # low + increment,
                # True
        # ],
        # "icq2" : [
                # high + increment,
                # low + increment,
                # True
        # ],
        # "icq3" : [
                # high + increment,
                # low + increment,
                # True
        # ],
        # "lcp1" : [
                # high + increment,
                # low + increment,
                # True
        # ],
        # "lcp2" : [
                # high + increment,
                # low + increment,
                # True
        # ],
        # "lcp3" : [
                # high + increment,
                # low + increment,
                # True
        # ],
        # "lcq1" : [
                # high + increment,
                # low + increment,
                # True
        # ],
        # "lcq2" : [
                # high + increment,
                # low + increment,
                # True
        # ],
        # "lcq3" : [
                # high + increment,
                # low + increment,
                # True
        # ]
        # }
    # )
    # start = start + timedelta(minutes=1)
    # increment=increment+32
	
# start_date = "2016-09-30"
# stop_date = "2016-10-31"
# start = datetime.strptime(start_date, "%Y-%m-%d")
# stop = datetime.strptime(stop_date, "%Y-%m-%d")
# while start < stop:
    # rnd = randint(0,100)
    # db[analysis_config.EHUBDATAS].insert_one(
    # {
        # "u3" : 237.5+rnd,
        # "u2" : 234.39999389648438+rnd,
        # "u1" : 236+rnd,
        # "ir3" : 0.6000000238418579+rnd,
        # "ir2" : 0.6000000238418579+rnd,
        # "ir1" : 0.699999988079071+rnd,
        # "iq3" : 0+rnd,
        # "iq2" : 0+rnd,
        # "iq1" : 0.10000000149011612+rnd,
        # "id3" : 0.800000011920929+rnd,
        # "id2" : 0.800000011920929+rnd,
        # "id1" : 0.800000011920929+rnd,
        # "er3" : 3.299999952316284+rnd,
        # "er2" : 10.100000381469727+rnd,
        # "er1" : 15.800000190734863+rnd,
        # "eq3" : 2.799999952316284+rnd,
        # "eq2" : 14.100000381469727+rnd,
        # "eq1" : 17.700000762939453+rnd,
        # "ed3" : 3.200000047683716+rnd,
        # "ed2" : 2.4000000953674316+rnd,
        # "ed1" : 13.699999809265137+rnd,
        # "pvp" : 448.3399963378906+rnd,
        # "lp" : 6612.8193359375+rnd,
        # "ts" : start,
        # "id" : "78:a5:04:ff:40:bb",
        # "__v" : 0
    # })
    # start = start + timedelta(seconds=1)

# insert base_load to use.
start_date = "2016-09-30"
stop_date = "2016-10-15" # don't fill the whole month or no new data will need to be generated
start = datetime.strptime(start_date, "%Y-%m-%d")
stop = datetime.strptime(stop_date, "%Y-%m-%d")
while start < stop:
    rnd = randint(1000,1889)
    base.mdb_insert_base_load_calc(
    {
        'starttime':start,
        'ts':start.timestamp(),
        'id' : "78:a5:04:ff:40:bb",
        'rbpL2':0.9147616135599069+rnd,
        'abpL1':0.7444922376840492+rnd,
        'rbpL3':0.12450118522699438+rnd,
        'abpL3':0.07940632555491955+rnd,
        'abpL2':0.5633437299304935+rnd,
        'abp':0.6577743055645574+rnd,
        'rbpL1':0.7517674074759381+rnd,
        'rbp':0.0417654186690638+rnd,
    })
    start = start + timedelta(days=1)
	 

