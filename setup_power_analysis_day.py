#!/usr/bin/env python3
import power_analysis_day
import json
from datetime import datetime
from datetime import timedelta

pad = power_analysis_day

def main():
    pad.mdb_setup_poweranalysisday_job_collection()

if __name__ == "__main__":
    # execute only if run as a script
    main()
	
	







