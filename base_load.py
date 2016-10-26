#!/usr/bin/env python3
from numpy import *
import scipy.linalg
from sys import exit
from datetime import datetime
from datetime import date
from datetime import timedelta
from datetime import time

# xIn is the input matrix of measurement data (from base_load_test_data)
# preFilt is a scalar, default 50 (from modal dialog)
# precision is a scalar, default 50 (from modal dialog)

preFilt = 50
precision = 50
xIn_test_random = random.rand(100, 8)
xIn = xIn_test_random

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
    result = []
    # See acelog_loadTrend.m
    trendPeriod = 24*3600 # input["analysismodel"] == "POWERANALYSISDAY"
    if (input["analysismodel"] == "POWERANALYSISHOUR"):
        trendPeriod = 3600
    # mdb_get_base_load_values(energyhubid, starttime, endtime, trendperiod) # Get pre-calulated base load values
    # calculate_base_values(trendPeriod/analysisModel) # Run base load function on raw values between starttime and endtime grouped by trendPeriod
    d = input["starttime"] # Needs to be cleaned up to even day/hour - refactor this to common function
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
    # xOut = zeros(1, size(xIn,2)); 
    xOut = zeros((1,xIn.shape[1]))
    
      # if (size(xIn,1) <= preFilt) 
            # xOut = min(xIn, [], 1);
            # return;
        # end;
    if size(xIn.shape[0])<=preFilt:
        xOut = nanmin(xIn_test_random, 0)
        print(xOut)
        exit()

    # xFilt = filter(1/preFilt*ones(1,preFilt), 1, abs(xIn)); % abs(x) mapped to xIn
    # xFilt = xFilt(2*preFilt+1:end, :); %redefine xFilt as selection from xFilt starting at 2*preFilt+1 to end, and everything in dim 2 (?)

    xFilt = lfilter(b=1/preFilt*ones(preFilt), a=1, x=abs(xIn))
    xFilt = xFilt[2*preFilt:,:]  

    # minVal = round(min(xFilt, [], 1)/precision)*precision; 

    minVal = round(nanmin(xFilt,0)/precision)*precision

    # sigVal = floor(std(xFilt, 0, 1)/precision)*precision;
    #std([1,3,4,6], ddof=1)
    sigVal = floor(std(xFilt, ddof=1)/precision)*precision;

    #	for i=1:size(xFilt,2)
    for i in range(0, xfile.shape[1]-1):
        bins = minVal(i) + linspace(0,sigVal(i), floor(sigVal(i)/precision))  #hi,low,count => 0,sigVal(i), floor(sigVal(i)/precision 
    #        bins = minVal(i)+(0:precision:sigVal(i)); #low:step:hi)

        histData = histogram(xFilt[:,i],bins)
    #		histData = histc(xFilt(:,i), bins); #digitize
            
        histPeakMargin = 0.01*sum(histData)
    #        histPeakMargin = 0.01*sum(histData);
    #        r = find(histData > histPeakMargin, 1);
        r = nonzero(histData > histPeakMargin)[0] # Needs better equivalent? 
	
        if r.all(x==0):
            xOut[i] = bins[r]
        else:
            xOut[i] = nanmin(xFilt(i))
    		
    #        if (~isempty(r))
    #            xOut(i) = bins(r);
    #        else
    #            xOut(i) = min(xFilt(:,i));
    #        end;
            
    #        xOut(i) = sign(mean(xIn(:,i)))*xOut(i);
    
        xOut[i] = sign(mean(xIn[:,i]))*xOut[i]	    

    print(xOut)
    return xOut
	
    if __name__ == "__main__":
        # execute only if run as a script
        base_val(xIn, preFilt, precision)
