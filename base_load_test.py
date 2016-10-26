#!/usr/bin/env python3
from numpy import *
import scipy.linalg

from sys import exit

# xIn is the input matrix of measurement data (from base_load_test_data)
# preFilt is a scalar, default 50 (from modal dialog)
# precision is a scalar, default 50 (from modal dialog)

preFilt = 50
precision = 50
xIn_test_random = random.rand(100, 8)
xIn = xIn_test_random

def base_val(xIn, preFilt, precision):
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
