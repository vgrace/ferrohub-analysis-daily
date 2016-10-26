#!/usr/bin/env python3
from numpy import *
import scipy.linalg
import scipy.signal
from scipy.signal import lfilter

xIn_test_random = random.rand(100, 8)
print(xIn_test_random)
print(size(xIn_test_random,1))
print(xIn_test_random.shape) 
print(xIn_test_random.shape[0]) # size(xIn,1)
print(xIn_test_random[98:100][:,0:4])

# xOut = zeros(ndarray.size(xIn)) or xIn.shape[1] (?)
# xOut = zeros(1, size(xIn,2)); 
xOut = zeros((1,xIn_test_random.shape[1]))
print (xOut)
print (xOut.shape)	
# min(xIn_test_random, [], 1) Smallest Element in Each Matrix row 
print(nanmin(array([[1.,2.,3.], [4.,5.,6.]]), 0)) # Minimum element in each row
xOut = nanmin(xIn_test_random, 0) # Minimum element in each row
print (xOut)
print (xOut.shape)
print(abs(xIn_test_random))
print(1/50*ones((1,50)))
#% scipy.signal.lfilter(b=1/preFilt*ones(1,preFilt), 1, x=abs(xIn)) [, axis=-1, zi=None)]
#	xFilt = filter(1/preFilt*ones(1,preFilt), 1, abs(xIn)); % abs(x) mapped to xIn
xFilt = lfilter(b=1/50*ones(50), a=1, x=abs(xIn_test_random))
print(xFilt)
# xFilt = xFilt(2*preFilt+1:end, :)
xFilt = xFilt[2:,:]

print(std(xFilt, ddof=1))

print(linspace(0,2,6))
#print(isempty(xOut))
print(sum(xFilt))
print(nonzero(xFilt >= 0)[0] )


myarray = array([[1.,2.,3.,4], [4.,5.,6.,7], [7.,8.,9.,10],[10,11,12,13]])
print(myarray[:,0])
print(myarray[0,:])
print(myarray[1:3,:])
myarray[1:3,:]=array([[8,8,8,8],[9,9,9,9]])
print(myarray)