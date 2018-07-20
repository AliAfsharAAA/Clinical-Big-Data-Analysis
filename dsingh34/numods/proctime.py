# Module for  prrocessing the time sequences.


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

from .usefuns import goodstr, goodfreq, AinB, tmdfAfromB, drawline, compSeqScalar
from .numio import getmetadf, gettimedf


def givenanondict(timeseries, fs, dtstart, timestr=False):

	nandex = np.argwhere(np.isnan(timeseries)).flatten()
	nondex = np.argwhere(~np.isnan(timeseries)).flatten()
	
	nan_ts = np.split(nandex, np.where(np.diff(nandex) != 1)[0] + 1)
	non_ts = np.split(nondex, np.where(np.diff(nondex) != 1)[0] + 1)
	
	freq = goodfreq(fs)
	time = np.rint(np.arange(len(timeseries)) / freq)
	
	nansize = 0
	nonsize = 0
	nanlen = list()
	nonlen = list()
	nanrange = list()
	nonrange = list()
	nantimestamp = list()
	nontimestamp = list()
	
	if len(nan_ts[0]) > 0:
		for nan in nan_ts:
			nansize += 1
			nanlen.append(nan.size)
			nanrange.append((nan[0], nan[-1]))
			nanstarttime = (dtstart + datetime.timedelta(seconds=time[nan[0]]))
			nanendtime = (dtstart + datetime.timedelta(seconds=time[nan[-1]]))
			if timestr:
				nanstarttime = nanstarttime.strftime("%Y-%m-%d %H:%M:%S")
				nanendtime = nanendtime.strftime("%Y-%m-%d %H:%M:%S")
			nantimestamp.append((nanstarttime, nanendtime))
	
	if len(non_ts[0]) > 0:	
		for non in non_ts:
			nonsize += 1
			nonlen.append(non.size)
			nonrange.append((non[0], non[-1]))
			nonstarttime = (dtstart + datetime.timedelta(seconds=time[non[0]]))
			nonendtime = (dtstart + datetime.timedelta(seconds=time[non[-1]]))
			if timestr:
				nonstarttime = nonstarttime.strftime("%Y-%m-%d %H:%M:%S")
				nonendtime = nonendtime.strftime("%Y-%m-%d %H:%M:%S")
			nontimestamp.append((nonstarttime, nonendtime))
	
	nanondict = {
				"non_segments": nonsize,
				"non_lengths": nonlen,
				"non_ranges": nonrange,
				"non_timestamps": nontimestamp,
				"nan_segments": nansize,
				"nan_lengths": nanlen,
				"nan_ranges": nanrange,
				"nan_timestamps": nantimestamp
			}
	
	return nanondict

def giveartdict(artlist=None, timeseries=None, fs=None, dtstart=None, printflag=False):
	
	if artlist is None:
		print("Invalid data, provide correct data (list of tuples of indices with dimentionality 2).")
		return
	
	artdict = {
				"small_art_segments": 0,
				"small_art_lengths": [],
				"small_art_ranges": [],
				"big_art_segments": 0,
				"big_art_lengths": [],
				"big_art_ranges": [],
			}
	
	if len(artlist) == 0:
		if printflag: print("Empty list/no indices/no artifacts.")
		return artdict
	
	freq = goodfreq(fs)
	time = np.rint(np.arange(len(timeseries)) / freq)
	
	for art in artlist:
		
		starttime = (dtstart + datetime.timedelta(seconds=time[art[0]]))
		endtime = (dtstart + datetime.timedelta(seconds=time[art[1]]))
		
		hourflag = tmdfAfromB(starttime, endtime)["hourflag"]
		
		if hourflag:
			
			artdict["big_art_segments"] += 1
			artdict["big_art_lengths"].append(art[1] - art[0] + 1)
			artdict["big_art_ranges"].append(art)
			
		else:
			
			artdict["small_art_segments"] += 1
			artdict["small_art_lengths"].append(art[1] - art[0] + 1)
			artdict["small_art_ranges"].append(art)
	
	return artdict

def givetimedseq(signal_name=None, recname=None, timeunit="seconds", sequence=None, fs=None):
	
	if type(signal_name) == str and type(recname) == str and sequence == None and fs == None:
		flag = 1
	elif (type(sequence) == np.ndarray and len(sequence.shape) == 1) and (type(fs) == float or type(fs) == np.float64 or type(fs) == int or type(fs) == np.int64) and signal_name == None and recname == None:
		flag = 2
	else:
		print("Invalid data provided, provide correct data.")
		return
	
	division_factor = {"seconds": 1, "minutes": 60, "hours": 60*60}
	
	if timeunit not in division_factor.keys():
		print("Incorrect time unit, choose from 'seconds', 'minutes' or 'hours'.")
		return
	
	if flag == 1:
	
		metadf = getmetadf(signal_name)
		timedf = gettimedf(signal_name)
		
		freq = goodfreq(metadf.loc[recname]["sampling_frequency"])
		seq = timedf.loc[recname]["sequence"]
		time = np.rint(np.arange(metadf.loc[recname]["signal_length"]) / freq)
		
	if flag == 2:
		
		freq = goodfreq(fs)
		seq = sequence
		time = np.rint(np.arange(seq.size) / freq)
		
	if timeunit in division_factor.keys(): 
		unit_indices = np.where(time/division_factor[timeunit] == np.rint(time/division_factor[timeunit]))
		newtime = range(unit_indices[0].size)
		newseq = seq[unit_indices]
	
	if flag == 1: return newtime, newseq
	if flag == 2: return unit_indices[0]

def consecdiff(sequence=None, fs=None, timeunit="seconds", threshold=None):
	
	if type(sequence) != np.ndarray or (type(sequence) == np.ndarray and len(sequence.shape) != 1) or (type(fs) != float and type(fs) != np.float64 and type(fs) != int and type(fs) != np.int64) or (type(threshold) != float and type(threshold) != int) or timeunit not in ["seconds", "minutes", "hours"]:
		print("Invalid data provided, provide correct data.")
		return
	
	get_timed_indices = givetimedseq(sequence=sequence, fs=fs, timeunit=timeunit)
	get_timed_vals = sequence[get_timed_indices]
	get_shifted_vals = np.concatenate([[0.0], get_timed_vals])[:-1]
	get_diff = np.abs(get_timed_vals - get_shifted_vals)[1:]
	get_outofbounds = compSeqScalar(get_diff, threshold, ">")
	get_complete_outofbounds = np.concatenate([get_outofbounds, get_outofbounds + 1])
	get_complete_sorted_outofbounds = np.sort(get_complete_outofbounds)
	get_complete_sorted_unique_outofbounds = np.unique(get_complete_sorted_outofbounds)
	
	return list(get_complete_sorted_unique_outofbounds)

def packnanoninfo(signal_name, rec_names, makedir="minedsets"):

	metadf = getmetadf(signal_name)

	if metadf is None:
		print("File does not exist/incorrect signal name.")
		return

	colnames = metadf.columns.values
	dictvals = ["non_segments", "non_lengths", "non_ranges", "non_timestamps", "nan_segments", "nan_lengths", "nan_ranges", "nan_timestamps"]

	if AinB(dictvals, colnames, "all"):

		if type(rec_names) == list:

			if len(rec_names) == 0:     
				print("Empty list of record names.")
				return

			elif AinB(rec_names, list(metadf.index.values), "any") and not AinB(rec_names, list(metadf.index.values), "all"):

				print("All records not present, returning sequence information for ones present.")

				rec_names = list(set(rec_names) & set(list(metadf.index.values)))

				seqinfodict = dict()
				for rec in rec_names:
					seqinfodict[rec] = metadf.loc[rec][dictvals].to_dict()

				return seqinfodict

			elif AinB(rec_names, list(metadf.index.values), "all"):

				seqinfodict = dict()
				for rec in rec_names:
					seqinfodict[rec] = metadf.loc[rec][dictvals].to_dict()

				return seqinfodict

			elif not AinB(rec_names, list(metadf.index.values), "any"):  
				print("None of the records present in the signal.")
				return

		elif type(rec_names) == str:

			if rec_names in list(metadf.index.values):

				return metadf.loc[rec_names][dictvals].to_dict()

			else:
				print("Record not present.")
				return

		else:
			print("Records entered incorrectly. Enter either a record name or a list of record names.")
			return

	else:
		print("All required data not present.")
		return

def classifynans(nanondict, nanflag=True, nonflag=True, printflag=False):
	
	if type(nanondict) is dict:
		
		if nanflag and nonflag:
			dictvals = ["non_segments", "non_lengths", "non_ranges", "non_timestamps", "nan_segments", "nan_lengths", "nan_ranges", "nan_timestamps"]
		elif nanflag and not nonflag:
			dictvals = ["nan_segments", "nan_lengths", "nan_ranges", "nan_timestamps"]
		elif not nanflag and nonflag:
			dictvals = ["non_segments", "non_lengths", "non_ranges", "non_timestamps"]
		
		if nanflag and nonflag:
			if AinB(dictvals, nanondict.keys(), "all"):
				classifiednans =    {
										"start_nan": [],
										"end_nan": [],
										"mid_nan_hour_segments": 0,
										"mid_nan_hour_lengths": [],
										"mid_nan_hour_ranges": [],
										"mid_nan_not_hour_segments": 0,
										"mid_nan_not_hour_lengths": [],
										"mid_nan_not_hour_ranges": []
									}
				if nanondict["nan_segments"] > 0:
					
					for nan_range in nanondict["nan_ranges"]:
						
						seq_end = sum(nanondict["nan_lengths"]) + sum(nanondict["non_lengths"]) - 1
						
						nan_range_index = nanondict["nan_ranges"].index(nan_range)
						hourflag = tmdfAfromB(nanondict["nan_timestamps"][nan_range_index][0], nanondict["nan_timestamps"][nan_range_index][1])["hourflag"]
						
						if nan_range[0] == 0:
							classifiednans["start_nan"].append(nan_range)
						if nan_range[1] == seq_end:
							classifiednans["end_nan"].append(nan_range)
						
						if hourflag and nan_range[0] != 0 and nan_range[1] != seq_end:
							classifiednans["mid_nan_hour_lengths"].append(nan_range[1] - nan_range[0] + 1)
							classifiednans["mid_nan_hour_ranges"].append(nan_range)
						elif not hourflag and nan_range[0] != 0 and nan_range[1] != seq_end:
							classifiednans["mid_nan_not_hour_lengths"].append(nan_range[1] - nan_range[0] + 1)
							classifiednans["mid_nan_not_hour_ranges"].append(nan_range)
							
					classifiednans["mid_nan_hour_segments"] = len(classifiednans["mid_nan_hour_ranges"])
					classifiednans["mid_nan_not_hour_segments"] = len(classifiednans["mid_nan_not_hour_ranges"])
							
					return classifiednans
							
				else:
					if printflag:
						print("No nan sequences.")
					return classifiednans
			
			else:
				print("Incorrect dictionary/all information not present.")
				return
			
		else:
			print("Incorrect dictionary/all information not present.")
			return
		
	else:
		print("Incorrect object type, give a dictionary of nan and non data.")
		return

def classifyarts(signal_name=None, sequence=None, fs=None, invalids=None, printflag=False):
	
	freq2timedict = {1.0: "seconds", 60.0: "minutes", 60.0*60.0: "hours"}
	
	times = ["seconds", "minutes", "hours"]
	
	if (type(fs) != float and type(fs) != np.float64 and type(fs) != int and type(fs) != numpy.int64) or ((type(fs) == float or type(fs) == np.float64 or type(fs) == int or type(fs) == numpy.int64) and np.rint(1.0/fs) not in freq2timedict) or type(signal_name) != str or type(sequence) != np.ndarray or (type(sequence) == np.ndarray and ((len(sequence.shape) == 2 and sequence.shape[0] != 2) and len(sequence.shape) != 1)):
		print("Invalid data provided, provide correct data.")
		return
	
	goodsignalname = goodstr(signal_name)
	
	get_low_high = lambda (low, high), timeseries: sum([list(compSeqScalar(timeseries, low, "<=")), list(compSeqScalar(timeseries, high, ">="))], [])
	get_diff_low_high = lambda (low, high), timeseries: sum([list(compSeqScalar(timeseries[0] - timeseries[1], low, "<=")), list(compSeqScalar(timeseries[0] - timeseries[1], high, ">"))], [])
	get_consec_diff = lambda sequence, fs, timeunit, threshold: consecdiff(sequence, fs, timeunit, threshold)
	grp_remreps_sort_indices = lambda rangelist: np.sort(np.array(list(set(tuple(sum(rangelist, []))))))
	get_ranges = lambda goodindices: np.split(goodindices, np.where(np.diff(goodindices) != 1)[0] + 1)
	get_range_tups = lambda goodranges: [(r[0], r[-1]) for r in goodranges if len(r) > 0]
	#get_range_tups = lambda goodranges: goodranges
	
	pipeline = lambda siglist: get_range_tups(get_ranges(grp_remreps_sort_indices(siglist)))
	
	if goodsignalname in ["abpsys", "abpdias"]:
		
		if len(sequence.shape) == 2:
			
			if goodsignalname == "abpsys":
				sigseq = sequence[0]
				invalid_ranges, invalid_diff_ranges, thresholds = ((20.0, 300.0), (5.0, 200.0), [20.0, 80.0])
			elif goodsignalname == "abpdias": 
				sigseq = sequence[1]
				invalid_ranges, invalid_diff_ranges, thresholds = ((5.0, 225.0), (5.0, 200.0), [20.0, 80.0])
			
			if invalids is not None:
				try:
					invalid_ranges, invalid_diff_ranges, thresholds = invalids
				except:
					print("Incorrect invalid range sequence provided, using default ranges.")
					
			siglist = [get_low_high(invalid_ranges, sigseq), get_diff_low_high(invalid_diff_ranges, sequence)]
			
			for i in range(len(thresholds)):
				siglist.append( get_consec_diff(sigseq, fs, times[i], thresholds[i]) )
				
			return pipeline(siglist)
		
		elif len(sequence.shape) == 1:
			
			if goodsignalname == "abpsys":
				invalid_ranges, thresholds = ((20.0, 300.0), [20.0, 80.0])
			elif goodsignalname == "abpdias": 
				invalid_ranges, thresholds = ((5.0, 225.0), [20.0, 80.0])
			
			if invalids is not None:
				try:
					invalid_ranges, thresholds = invalids
				except:
					print("Incorrect invalid range sequence provided, using default ranges.")
					
			siglist = [get_low_high(invalid_ranges, sequence)]
			
			for i in range(len(thresholds)):
				siglist.append( get_consec_diff(sequence, fs, times[i], thresholds[i]) )
				
			return pipeline(siglist)
		
	elif goodsignalname == "abpmean":
			
			if len(sequence.shape) != 1:
				
				print("Numpy sequence should be 1 dimentional for signal '" + goodsignalname + "'.")
				return
			
			else:
	 
				thresholds = [20.0, 80.0]
				
				if invalids is not None:
					try:
						thresholds = invalids
					except:
						print("Incorrect invalid range sequence provided, using default ranges.")
						
				siglist = []
				
				for i in range(len(thresholds)):
					siglist.append( get_consec_diff(sequence, fs, times[i], thresholds[i]) )
					
			return pipeline(siglist)
	
	elif goodsignalname == "hr":
		
		if len(sequence.shape) != 1:
			
			print("Numpy sequence should be 1 dimentional for signal '" + goodsignalname + "'.")
			return
		
		else:
 
			invalid_ranges = (20.0, 220.0)
			
			if invalids is not None:
				try:
					invalid_ranges = invalids
				except:
					print("Incorrect invalid range sequence provided, using default ranges.")
					
			siglist = [get_low_high(invalid_ranges, sequence)]
				
		return pipeline(siglist)
	
	else:
		if printflag: print("Invalid signal name/signal artifact classification not available.")
		return []

def interpolate(to_ip_ranges=None, sequence=None, printflag=False):

	if (type(to_ip_ranges) != list or type(sequence) != np.ndarray) or (to_ip_ranges == None or sequence is None):
		print("Invalid data provided, provide correct data.")
		return

	elif len(to_ip_ranges) == 0:
		if printflag: print("No interpolating ranges.")
		return sequence

	else:
		for to_ip_range in to_ip_ranges:
			sequence[to_ip_range[0]:to_ip_range[1]+1] = drawline(to_ip_range, sequence)
		return sequence

def nanbigarts(bigartranges=None, sequence=None, printflag=False):

	if (type(bigartranges) != list or type(sequence) != np.ndarray) or (bigartranges == None or sequence is None):
		print("Invalid data provided, provide correct data.")
		return
	
	elif len(bigartranges) == 0:
		if printflag: print("No ranges to fill nans in.")
		return sequence
	
	else:
		to_nan_indices = sum(map(lambda nrange: range(nrange[0], nrange[1]+1), bigartranges), [])
		sequence[to_nan_indices] = np.nan
	
		return sequence