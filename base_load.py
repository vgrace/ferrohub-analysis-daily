#!/usr/bin/env python3
import sys
from numpy import *
from  scipy.linalg import *
from  scipy.signal import *
from sys import exit
from datetime import datetime
from datetime import date
from datetime import timedelta
from datetime import time
import mdb_base_load
from datetime_utilities import *
from power_analysis_day import unsigned64int_from_words

# xIn is the input matrix of measurement data (from base_load_test_data)
# preFilt is a scalar, default 50 (from modal dialog)
# precision is a scalar, default 50 (from modal dialog)

preFilt = 50
precision = 50
xIn_test_random = random.rand(100, 8)
xIn = xIn_test_random
isDebugMode = False

def get_base_load_values(input):
    """
	Load the base values for this period described in the input
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
	
	Returns:
		abp == Active 3P Base AP(active)LL(line_to_line)Load  [kW] - from Preal
        rbp == Reactive 3P Base AQ(reactiveLL(line_to_line)Load  [VAr] - from Pimag
        abpL1 == Active 1P Base AP(active)L1N(neutral)Load  [kW] - From PLoad[n]
        abpL2 == Active 1P Base AP(active)L2N(neutral)Load  [kW]
        abpL3 == Active 1P Base AP(active)L3N(neutral)Load  [kW]
        rbpL1 == Reactive 1P Base AQ(reactive)L1N(neutral)Load [VAr] - From QLoad[n]
        rbpL2 == Reactive 1P Base AQ(reactive)L2N(neutral)Load [VAr]
        rbpL3 == Reactive 1P Base AQ(reactive)L3N(neutral)Load [VAr]
    """
    return list(mdb_base_load.mdb_get_base_load_calc(input["energyhubid"], input["starttime"], input["endtime"]))


def get_base_load_values_mock(input):
    """
	Load the base values for this period described in the input
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
	
	Returns:
		abp == Active 3P Base AP(active)LL(line_to_line)Load  [kW] - from Preal
        rbp == Reactive 3P Base AQ(reactiveLL(line_to_line)Load  [VAr] - from Pimag
        abpL1 == Active 1P Base AP(active)L1N(neutral)Load  [kW] - From PLoad[n]
        abpL2 == Active 1P Base AP(active)L2N(neutral)Load  [kW]
        abpL3 == Active 1P Base AP(active)L3N(neutral)Load  [kW]
        rbpL1 == Reactive 1P Base AQ(reactive)L1N(neutral)Load [VAr] - From QLoad[n]
        rbpL2 == Reactive 1P Base AQ(reactive)L2N(neutral)Load [VAr]
        rbpL3 == Reactive 1P Base AQ(reactive)L3N(neutral)Load [VAr]
    """
    result = []
    # See acelog_loadTrend.m
    trendPeriod = 24*3600 # input["analysismodel"] == "POWERANALYSISDAY"
    if (input["analysismodel"] == "POWERANALYSISHOUR"):
        trendPeriod = 3600
    # mdb_get_base_load_values(energyhubid, starttime, endtime, trendperiod) # Get pre-calulated base load values
    # calculate_base_values(trendPeriod/analysisModel) # Run base load function on raw values between starttime and endtime grouped by trendPeriod
    d = round_down_datetime(input["starttime"]) # Needs to be cleaned up to even day/hour - refactor this to common function
    delta = timedelta(days=1)
    if (input["analysismodel"] == "POWERANALYSISHOUR"):
        delta = timedelta(hour=1)
    while d <= input["endtime"]:
        resultitem = {}
        resultitem["ts"]=d
        resultitem["abp"]=random.rand()
        resultitem["abpL1"]=random.rand()
        resultitem["abpL2"]=random.rand()
        resultitem["abpL3"]=random.rand()
        resultitem["rbp"]=random.rand()
        resultitem["rbpL1"]=random.rand()
        resultitem["rbpL2"]=random.rand()
        resultitem["rbpL3"]=random.rand()
        result.append(resultitem)
        d += delta
    return sorted(result, key=lambda x: x["ts"].isoformat())

def base_val(xIn, preFilt, precision):
    """
    From the matrix of the values during a period (xIn(values,units))
    
    % Single phase and 3-phase Load Active and Reactive Power
    % Output: N x (8+1) Matrix    
    newLogData = zeros(aceData.N, 9); 
    newLogData(:, 1) = aceData.TS;
    newLogData(:, 2) = Preal;
    newLogData(:, 3) = Pimag; 
    newLogData(:, 4:6) = PLoad;
    newLogData(:, 7:9) = QLoad;
    Select INDEX = [2:9];        % {Single phase/3-phase active/reactive power}
    
    calculate the base values for the period.
	
	Returns: 
		abp == Active 3P Base AP(active)LL(line_to_line)Load  [kW] - from Preal
        rbp == Reactive 3P Base AQ(reactiveLL(line_to_line)Load  [VAr] - from Pimag
        abpL1 == Active 1P Base AP(active)L1N(neutral)Load  [kW] - From PLoad[n]
        abpL2 == Active 1P Base AP(active)L2N(neutral)Load  [kW]
        abpL3 == Active 1P Base AP(active)L3N(neutral)Load  [kW]
        rbpL1 == Reactive 1P Base AQ(reactive)L1N(neutral)Load [VAr] - From QLoad[n]
        rbpL2 == Reactive 1P Base AQ(reactive)L2N(neutral)Load [VAr]
        rbpL3 == Reactive 1P Base AQ(reactive)L3N(neutral)Load [VAr]
    """ 
    print("#base_val(xIn,",preFilt,",",precision,")")
    print("xIn.shape=",xIn.shape)
    print("xIn=",xIn)
    # xOut = zeros(1, size(xIn,2)); 
    xOut = zeros((xIn.shape[1]))
    
      # if (size(xIn,1) <= preFilt) 
            # xOut = min(xIn, [], 1);
            # return;
        # end;
  
    if xIn.shape[0]<=preFilt:
        xOut = nanmin(xIn, 0)
        print("size(xIn.shape[0])<=preFilt: xOut=", xOut)
        return xOut
    #print("xOut=",xOut)
    # xFilt = filter(1/preFilt*ones(1,preFilt), 1, abs(xIn)); % abs(x) mapped to xIn
    # xFilt = xFilt(2*preFilt+1:end, :); 
    
    # print("1/preFilt*ones(preFilt)")	
    # print(1/preFilt*ones(preFilt))
    # print("x=abs(xIn)")
    # print(abs(xIn))
    xFilt = lfilter(b=1/preFilt*ones(preFilt), a=1, x=abs(xIn))
    #print("xFilt.shape=",xFilt.shape)
    xFilt = xFilt[2*preFilt:,:]  
    #print("xFilt.shape=",xFilt.shape)
    #print("xFilt=",xFilt)
    # minVal = round(min(xFilt, [], 1)/precision)*precision; 
    # TypeError: type numpy.ndarray doesn't define __round__ method
    # minVal = round(nanmin(xFilt,0)/precision)*precision
    #print("amin(xFilt,axis=0)",amin(xFilt,axis=0))
	
    minVal	= array(list(map(round, amin(xFilt,axis=0)/precision)))*precision

    #print("minVal",minVal)

    # sigVal = floor(std(xFilt, 0, 1)/precision)*precision;
    #std([1,3,4,6], ddof=1)
    #print("std(xFilt, axis=0, ddof=1)",std(xFilt, axis=0, ddof=1))
    sigVal = floor(std(xFilt, axis=0, ddof=1)/precision)*precision;
    #print("minVal=",minVal)
    #print("sigVal=",sigVal)
    #	for i=1:size(xFilt,2)
    #print("range(0, xFilt.shape[1]-1)=",range(0, xFilt.shape[1]-1))	
    #print("range(0, xFilt.shape[1])",range(0, xFilt.shape[1]))
    for i in range(0, xFilt.shape[1]-1):
        #print("i=",i)
        #bins = minVal[i] + linspace(0,sigVal[i], floor(sigVal[i]/precision))  #hi,low,count => 0,sigVal(i), floor(sigVal(i)/precision 
    #        bins = minVal(i)+(0:precision:sigVal(i)); #low:step:hi)
        bins =  minVal[i] + arange(0,sigVal[i]+precision+0.0001,precision)
        #print("arange(0,sigVal[i]+precision+0.0001,precision)=",arange(0,sigVal[i]+precision+0.0001,precision))
        #print("xFilt[:,i]=",xFilt[:,i])
        #print("bins=",bins)
        histData = histogram(xFilt[:,i],bins) # Return the count of values that belong in each of the bins.
    #	histData = histc(xFilt(:,i), bins); #digitize? -> bincount
	# bincounts = histc(x,binranges) counts the number of values in x that are within each specified bin range. 
	# The input, binranges, determines the endpoints for each bin. The output, bincounts, contains the number of elements from x in each bin.
	# If x is a vector, then histc returns bincounts as a vector of histogram bin counts.
	# If x is a matrix, then histc operates along each column of x and returns bincounts as a matrix of histogram bin counts for each column.
	# To plot the histogram, use bar(binranges,bincounts,'histc').
	# https://docs.scipy.org/doc/numpy/reference/generated/numpy.histogram.html
        #print("histData=",histData[0])  
        #print("histData=",histData[1])  		
        histPeakMargin = 0.01*sum(histData[0])
    #        histPeakMargin = 0.01*sum(histData);
    #        r = find(histData > histPeakMargin, 1);
        #print("nonzero(histData[0] > histPeakMargin)=",nonzero(histData[0] > histPeakMargin))
        r = nonzero(histData[0] > histPeakMargin)[0] # Needs better equivalent? 
        #print("r=",r)  	
        #print("r.size=",r.size)  
        #print("xOut=",xOut)     
        if r.size!=0: # Correct interpretation?
            print("bins[r]=",bins[r])
            xOut[i] = mean(bins[r]) # This sometimes results in an array?
        else:
            xOut[i] = nanmin(xFilt[i])
    		
    #        if (~isempty(r))
    #            xOut(i) = bins(r);
    #        else
    #            xOut(i) = min(xFilt(:,i));
    #        end;
            
    #        xOut(i) = sign(mean(xIn(:,i)))*xOut(i);
    
        xOut[i] = sign(mean(xIn[:,i]))*xOut[i]	    
    print("xOut=",xOut)     
    return xOut

def transform_raw_data():
    """
	Transforms the input from the main measurement database collection ehubdatas 
    to the format required by the base_val() function (the xIn parameter)
	
	ux - external voltage, x - phase.
    i  - internal
    e  - external 
    r  - rms
    q  - active 
    d  - reactive 
    all numbers represent which phase. 
    id : mac_address. 
    ts : timestamp 
    lp - loadpower
    pvp - pv -power.
	"""
    result = []
    return results

def fill_in_missing_energy_counters(ec_data):
    for counter in ec_data:
        # Fetch the values
        lcp1 = ec_data.get("lcp1")
        lcp2 = ec_data.get("lcp2")
        lcp3 = ec_data.get("lcp3")
        lcq1 = ec_data.get("lcq1")
        lcq2 = ec_data.get("lcq2")
        lcq3 = ec_data.get("lcq3")
        # Save the ones that exist
        # If there are missing values, use the last one

def transform_energy_counter(ec_data):
    """    newLogData(:, 1) = aceData.TS;
    newLogData(:, 2) = Preal; # ==(lcp1+lcp2+lcp3) (delta mellan två ts)
    newLogData(:, 3) = Pimag; # ==(lcq1+lcq2+lcq3) (delta mellan två ts)
    newLogData(:, 4:6) = PLoad; # ==lcp[1-3]
    newLogData(:, 7:9) = QLoad; # ==lcq[1-3]
    """
    #ec_data=fill_in_missing_energy_counters(ec_data)
    result = zeros((len(ec_data),9))
    last = ec_data[0]
    i=0
    for current_ec in ec_data[1:]:
        # print(current_ec)
        ts = last["ts"]
        span  = current_ec["ts"] - ts
        if ts>last["ts"]:
            print("WRONG ORDER FOR TIMESTAMPS",span.seconds)
        # lcp1 in mJ, convert to kW : P(kW) = E(J) / (1000 × t(s))  : kW=mJ/(1000*1000*span.seconds) 
        if (span.seconds>0):
            try:
                PLoad1 = (unsigned64int_from_words(current_ec["lcp1"][0],current_ec["lcp1"][1], not(current_ec["lcp1"][2]))  - unsigned64int_from_words(last["lcp1"][0],last["lcp1"][1], not(last["lcp1"][2])))/(span.seconds*1000000)
                PLoad2 = (unsigned64int_from_words(current_ec["lcp2"][0],current_ec["lcp2"][1], not(current_ec["lcp2"][2]))  - unsigned64int_from_words(last["lcp2"][0],last["lcp2"][1], not(last["lcp2"][2])))/(span.seconds*1000000)
                PLoad3 = (unsigned64int_from_words(current_ec["lcp3"][0],current_ec["lcp3"][1], not(current_ec["lcp3"][2]))  - unsigned64int_from_words(last["lcp3"][0],last["lcp3"][1], not(last["lcp3"][2])))/(span.seconds*1000000)
                QLoad1 = (unsigned64int_from_words(current_ec["lcq1"][0],current_ec["lcq1"][1], not(current_ec["lcq1"][2]))  - unsigned64int_from_words(last["lcq1"][0],last["lcq1"][1], not(last["lcq1"][2])))/(span.seconds*1000000)
                QLoad2 = (unsigned64int_from_words(current_ec["lcq2"][0],current_ec["lcq2"][1], not(current_ec["lcq2"][2]))  - unsigned64int_from_words(last["lcq2"][0],last["lcq2"][1], not(last["lcq2"][2])))/(span.seconds*1000000)
                QLoad3 = (unsigned64int_from_words(current_ec["lcq3"][0],current_ec["lcq3"][1], not(current_ec["lcq3"][2]))  - unsigned64int_from_words(last["lcq3"][0],last["lcq3"][1], not(last["lcq3"][2])))/(span.seconds*1000000)
                Preal = PLoad1+PLoad2+PLoad3
                Pimag = QLoad1+QLoad2+QLoad3
                last = current_ec
                result[i,0]=ts.timestamp()
                result[i,1]=Preal
                result[i,2]=Pimag
                result[i,3]=PLoad1
                result[i,4]=PLoad2
                result[i,5]=PLoad3
                result[i,6]=QLoad1
                result[i,7]=QLoad2
                result[i,8]=QLoad3	
            except KeyError:
                print("transform_energy_counter() - KeyError ", ts)
        result[i,0]=ts.timestamp()
        i=i+1
    if len(result)>0:  
        print("[timestamp,Preal,Pimag,PLoad1,PLoad2,PLoad3,QLoad1,QLoad2,QLoad3]")	
        print(result[0])
    return result

def store_base_load(deviceid ,start,base_load_array):
    print("store_base_load(",deviceid,",",start)
    mdb_base_load.mdb_insert_base_load_calc(
	 {
        'starttime':start,
        'ts':start.timestamp(),
        'id' : deviceid,
        'abp':base_load_array[0],
        'rbp':base_load_array[1],
		'abpL1':base_load_array[2],
        'abpL2':base_load_array[3],
        'abpL3':base_load_array[4],	
        'rbpL1':base_load_array[5],		
        'rbpL2':base_load_array[6],
        'rbpL3':base_load_array[7],
    })

def run_base_load():
    print("# run_base_load()")
    last = mdb_base_load.mdb_get_last_inserted()
    for device in last:
        print("## Device ", device["id"])
        if device["last_starttime"] < round_down_datetime(datetime.today()):
            calc_date=device["last_starttime"]
            stop=round_down_datetime(datetime.today()) - timedelta(days=1)
            print("Last base load calculated ", calc_date, " calculating up to and including yesterday: ", stop)
            while calc_date < stop:
                energy_counters=list(mdb_base_load.mdb_get_base_load_energy_counter_data(device["id"], calc_date, round_up_datetime(calc_date)))
                if len(energy_counters)>0:				
                    date_data = transform_energy_counter(energy_counters)
                    # No sanity check for sufficient values here, put this in base_val				
                    date_base_load = base_val(date_data[:-1,1:9], 2, 2)	
                    store_base_load(device["id"],calc_date,date_base_load)
                calc_date = calc_date + timedelta(days=1)
				

if __name__ == "__main__":
    # execute only if run as a script
	run_base_load()
   