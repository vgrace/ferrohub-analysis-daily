#!/usr/bin/env python3
import analysis_config
import pymongo
from pymongo import CursorType
# from datetime import datetime
# from datetime import date
# from datetime import timedelta
# from datetime import time
from datetime_utilities import *
from bson.son import SON # As python dictionaries don’t maintain order you should use SON or collections.OrderedDict where explicit ordering is required eg “$sort”:
connection = pymongo.MongoClient(analysis_config.main_mongodb_uri)
db = connection[analysis_config.main_mongodb]
local_connection = pymongo.MongoClient(analysis_config.local_mongodb_uri)
local_db = local_connection[analysis_config.local_mongodb]

def mdb_get_base_load_data(device_mac, starttime, endtime):
    end_date =  date + timedelta(days=1)
    base_load_data = db.ehubdatas.find( { "id" : device_mac, "$and" : [{"ts": { "$lte" : end_date}}, {"$ts": { "$gte" : starttime}}])
    return base_load_data