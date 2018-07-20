# Module for generic useful functions used frequently.


import os
import sys
import csv
import argparse
import operator
import cPickle as pickle
import re
from mpi4py import MPI
from itertools import chain
from collections import defaultdict
from collections import Counter
import numpy as np
import pandas as pd
import wfdb
import matplotlib.pyplot as plt
import datetime
import time


def goodstr(string):
	
	return string.replace("_", "").replace(" ", "").lower()
	
def goodfreq(freq):
	
	return 1/np.rint(1/freq).astype(float)

def AinB(A, B, anyorall="all"):
	
	if len(A) > len(B):
		print("Length of second list in arguments should be greater than or equal to length of first list for this function to work.")
		return
		
	if anyorall == "any": return (len(set(A) & set(B)) > 0)
	elif anyorall == "all": return (len(set(A) & set(B)) == len(set(A)))
	else: print("Invalid choice."); return

def anyinAB(A, B):
	
	return (len(set(A) & set(B)) > 0)
	
def strL2intL(strL):
	
	intL = strL.replace("[","").replace("]","").replace("'","").split(", ")
	intL = [int(x) for x in intL if x.isdigit()]
	
	return intL
	
def strLT2intLT(strLT):
	
	intLT = re.split("[\(\)]", strLT.replace("[","").replace("]","").replace("'",""))
	intLT = [(int(x.split(", ")[0]), int(x.split(", ")[1])) for x in intLT if x != "" and x != ", "]
	
	return intLT
	
def strLT2dtLT(strLT):
	
	dtLT = re.split("[\(\)]", strLT.replace("[","").replace("]","").replace("'",""))
	dtLT = [(datetime.datetime.strptime(x.split(", ")[0], "%Y-%m-%d %H:%M:%S"), datetime.datetime.strptime(x.split(", ")[1], "%Y-%m-%d %H:%M:%S")) for x in dtLT if x != "" and x != ", "]
	
	return dtLT

def tmdfAfromB(A, B):
	
	# B should be greater than A
	
	hourflag = False
	
	diff = B - A
	
	secs = (diff.days * 24*60*60) + diff.seconds
	
	days = int(secs/86400)
	secs -= 86400*days

	hours = int(secs/3600)
	secs -= 3600*hours

	minutes = int(secs/60)
	secs -= 60*minutes
	
	if hours > 0 or days > 0: hourflag = True
	
	timediffdict = 	{
					"days": days,
					"hours": hours,
					"minutes": minutes,
					"seconds": secs,
					"hourflag": hourflag
				}
	return timediffdict

def delnumsigname(sigwithnum=None):

	remnum = lambda x: any(c.isalpha() for c in x)
	
	if type(sigwithnum) == list:
		removed = list()
		for sig in sigwithnum:
			if remnum(sig): removed.append(sig)
		return removed
	
	elif type(sigwithnum) == str:
		return remnum(sigwithnum)	
	
def joindicts(dict1, dict2, op=operator.add):
	
	return dict(dict1.items() + dict2.items() + [(key, op(dict1[key], dict2[key])) for key in set(dict1) & set(dict2)])

def removeduplicatesignals(olddict):
	
	# Obsolete. Not used anywhere.
	
	newkeylist = list(set(map(goodstr, olddict.keys())))

	newdict = defaultdict(list)
	newdict = newdict.fromkeys(newkeylist, [[],0,0,[],[]])

	for key,_ in newdict.items():
		newdict[key] = [[],0,0,[],[]]

	for oldkey in olddict.keys():

		newkey = goodstr(oldkey)

		newdict[newkey][0].append(oldkey)
		newdict[newkey][1] += olddict[oldkey][0]
		newdict[newkey][2] += olddict[oldkey][1]
		newdict[newkey][3].extend(olddict[oldkey][2])
		newdict[newkey][4].extend(olddict[oldkey][3])
		
	sig_ct_id = list()
	for item in newdict.items():
		sig_ct_id.append( [item[0]] + item[1] )
	
	return sig_ct_id

def drawline(range_idx=None, sequence=None):

	if (type(range_idx) != tuple or type(sequence) != np.ndarray) or (range_idx == None or sequence is None):
		print("Invalid data provided, provide correct data.")
		return

	x1_tmp = range_idx[0]
	x2_tmp = range_idx[1]

	if x1_tmp not in range(len(sequence)) or x2_tmp not in range(len(sequence)):
		print("Index privided is out of bounds.")
		return
	
	x1 = x1_tmp - 1
	x2 = x2_tmp + 1
	
	if x1_tmp == 0:
		if x2_tmp == len(sequence) - 1:
			y1 = sequence[x2_tmp]
		else:
			y1 = sequence[x2]
	else:
		y1 = sequence[x1]
	
	if x2_tmp == len(sequence) - 1:
		if x1_tmp == 0:
			y2 = sequence[x1_tmp]
		else:
			y2 = sequence[x1]
	else:
		y2 = sequence[x2]

	xvals = np.arange(x1+1, x2)

	m = (y2 - y1) / (x2 - x1)

	y = lambda x: np.around(m * (x - x1) + y1, decimals=2)

	return y(xvals)

def compSeqScalar(sequence=None, scalar=None, comparator=None):
	
	if type(sequence) != np.ndarray or (type(sequence) == np.ndarray and len(sequence.shape) != 1) or (type(scalar) != int and type(scalar) != np.int64 and type(scalar) != float and type(scalar) != np.float64) or type(comparator) != str or comparator not in ["<", "<=", ">", ">="] or(sequence is None or scalar is None or comparator is None):
		print("Invalid data provided, provide correct data.")
		return

	notnans = ~np.isnan(sequence)

	if comparator == "<": notnans[notnans] &= sequence[notnans] < scalar
	elif comparator == "<=": notnans[notnans] &= sequence[notnans] <= scalar
	elif comparator == ">": notnans[notnans] &= sequence[notnans] > scalar
	elif comparator == ">=": notnans[notnans] &= sequence[notnans] >= scalar

	return np.argwhere(notnans).flatten()

def argL2tupidx(sequence=None, comparator=None):
	
	if type(sequence) != np.ndarray or (type(sequence) == np.ndarray and len(sequence.shape) != 1) or type(comparator) != str or (type(comparator) == str and comparator not in ["nan", "notnan"]):
		print("Invalid data provided, provide correct data.")
		return
	
	if comparator is "nan": arglist = np.argwhere(np.isnan(sequence)).flatten()
	elif comparator is "notnan": arglist = np.argwhere(~np.isnan(sequence)).flatten()
	arglists = np.split(arglist, np.where(np.diff(arglist) != 1)[0] + 1)
	
	listnum = 0
	lenlist = []
	rangetups = []
	for ls in arglists:
		if ls.size > 0:
			listnum += 1
			lenlist.append(ls.size)
			rangetups.append((ls[0], ls[-1]))
	
	dct = {"segments": listnum, "lengths": lenlist, "ranges": rangetups}
	
	return dct