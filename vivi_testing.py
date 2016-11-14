import pymongo
import power_analysis_day
from datetime import datetime
from random import randint

pad = power_analysis_day
#client = pymongo.MongoClient("localhost", 27017)
#db = client.test


def main():

	# Original method
	job = {
	    "energyhubid": "78:a5:04:ff:40:bb",
	    "starttime": datetime(2016,10,1,0,0,0),
	    "endtime": datetime(2016,10,2,0,0,0),
	    "userid": "testuser",
	    "resultsid":"ABCD" + str(randint(0,1000)),
	    "analysismodel":"DAILYPOWER",
	    "jobstatus":0
	}; 

	##pad.mdb_insert_poweranalysisday_job(job)
	##data = pad.mdb_get_energy_counter_data_grouped(job)
	##base_values = []
	##hub_aggr = pad.get_energy_counter_aggregate(data, base_values)
	##print("------------------------------")
	##print(list(hub_aggr))


	# New method
	data = pad.mdb_get_energy_counter_data_new(job)
	base_values = []

	hug_aggr = pad.get_energy_counter_aggregate_new(data, base_values)
	
	print(list(hug_aggr))
	##print("------------------------------")
	##print(data)
	

	"""
{
  "energyhubid": "78:a5:04:ff:40:bb",
  "starttime": "2016-10-01T04:00:00.000Z",
  "endtime": "2016-10-01T09:00:00.000Z",
  "userid": "845"
}
"""
#print(db.poweranalysishour_jobs.insert_one(job).inserted_id)


#print(db.name)
#print(db.test_poweranalysisday_jobs)
#print(db.test_poweranalysisday_jobs.insert_one({"x": 8}).inserted_id)
#print(db.test_poweranalysisday_jobs.find_one())

#date = "2016-10-18 00:00:00"
#datestr = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
#print(datestr)
#date2 = "2016-10-18 23:00:00"
#date2str = datetime.strptime(date2, "%Y-%m-%d %H:%M:%S")
#print(date2str)
#print("Hello you handsome devil!")

if __name__ == "__main__":
    # execute only if run as a script
    main()