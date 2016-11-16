#!/usr/bin/env python3
import base_val
import mdb_base_val
from datetime_utilities import *

# xIn is the input matrix of measurement data (from base_load_test_data)
# preFilt is a scalar, default 50 (from modal dialog)
# precision is a scalar, default 50 (from modal dialog)

preFilt = 50
precision = 50/1000
xIn_test_random = random.rand(100, 8)
device_mac = "78:a5:04:ff:40:bb"
starttime=datetime(2016,11,7,0,0,0)
endtime=datetime(2016,11,10,0,0,0)
def base_val_test():
    mdb_get_base_load_raw_data(device_mac, starttime, endtime)

if __name__ == "__main__":
    # execute only if run as a script
    base_val_test(xIn, preFilt, precision)
