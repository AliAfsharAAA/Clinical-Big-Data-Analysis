# Module to get statistics on particular signals.


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

from .usefuns import goodstr, goodfreq, AinB, anyinAB
from .numio import getsignaldf, getmetadf, gettimedf, getsigdat


def sigdat_show(signal_name, proc=False, showcolnames=False, showsigdat=False, makedir="minedsets"):
	
	good_sig_name = goodstr(signal_name)
		
	sdfs = getsignaldf(makedir=makedir)
	mdfs = getmetadf(signal_name, makedir)
	tdfs = gettimedf(signal_name, proc, makedir)
	
	if showcolnames:
		print("\nSIGNALS COLUMNS:")
		print(list(sdfs.columns))
		print("\nMETADATA COLUMNS:")
		print(list(mdfs.columns))
		print("\nTIMESERIES COLUMNS:")
		print(list(tdfs.columns))
		
	if showsigdat:
		print("\nSIGNALS DATA:")
		print(sdfs[0:5])
		print("\nMETADATA DATA:")
		print(mdfs[0:5])
		print("\nTIMESERIES DATA:")
		print(tdfs[0:5])

def location_stats(signal_name, plot=False, fig_size=(8,6), makedir="minedsets"):
	
	good_sig_name = goodstr(signal_name)
	
	dfs = getsigdat(signal_name, False, makedir)
	
	if "location" not in list(dfs[1].columns) or "signal_id_names" not in list(dfs[0]):
		print("Information not available.")
		return
	
	else:  
		grouped = dfs[1]["location"].value_counts(dropna=False).to_dict()
		
		if plot:
			
			fig = plt.figure(figsize=fig_size)
			plt.pie(grouped.values(), labels=grouped.keys(), autopct="%1.1f%%")
			plt.suptitle("Signal: " + "/".join(dfs[0].loc[good_sig_name]["signal_id_names"]) + "\n" + "Percentage of records belonging to each hospital", fontsize=16)
			plt.show()
			
		return grouped

def freq_stats(signal_name, freq="s", plot=False, fig_size=(8,6), makedir="minedsets"):
	
	good_sig_name = goodstr(signal_name)
	
	dfs = getsigdat(signal_name, False, makedir)
	
	if freq == "s": column = "sampling_frequency"; fname = "sampling"
	elif freq == "c": column = "counter_frequency"; fname = "counter"
	else: print("Incorrect indicator for frequency. Select 's' for 'sampling_frequency' or 'c' for 'counter_frequency'."); return
	
	if column not in list(dfs[1].columns) or "signal_id_names" not in list(dfs[0]):
		print("Information not available.")
		return
	
	else:  
		grouped = dfs[1][column].value_counts(dropna=False).to_dict()
		
		if plot:
			
			fig = plt.figure(figsize=fig_size)
			plt.pie(grouped.values(),labels=grouped.keys(), autopct="%1.1f%%")
			plt.suptitle("Signal: " + "/".join(dfs[0].loc[good_sig_name]["signal_id_names"]) + "\n" + "Percentage of records of each " + fname + " frequency", fontsize=16)
			plt.show()
			
		return grouped

def numofsig_stats(signal_name, plot=False, bin_range=range(0,101,10), fig_size=(8,6), makedir="minedsets"):
	
	good_sig_name = goodstr(signal_name)
	
	dfs = getsigdat(signal_name, False, makedir)
	
	if "number_of_present_signals" not in list(dfs[1].columns) or "signal_id_names" not in list(dfs[0]):
		print("Information not available.")
		return
	
	else:
		counts, bin_edges = np.histogram(np.array(dfs[1]["number_of_present_signals"]), bins=bin_range)
		
		if plot:
			
			fig = plt.figure(figsize=fig_size)
			plt.hist(np.array(dfs[1]["number_of_present_signals"]), bins=bin_range, rwidth=0.9, color="b", alpha=0.5)
			plt.xlabel("Range of number of signals")
			plt.ylabel("Number of records")
			plt.suptitle("Signal: " + "/".join(dfs[0].loc[good_sig_name]["signal_id_names"]) + "\n" + "Number of records within ranges of number of signals", fontsize=16)
			plt.show()
			
		return counts, bin_edges

def siglen_stats(signal_name, plot=False, fig_size=(8,6), makedir="minedsets"):
	
	good_sig_name = goodstr(signal_name)
	
	dfs = getsigdat(signal_name, False, makedir)
	
	if "signal_length" not in list(dfs[1].columns) or "signal_id_names" not in list(dfs[0]):
		print("Information not available.")
		return
	
	else:
		
		bin_range=[0,10,100,1000,10000,100000,1000000]
		
		counts, bin_edges = np.histogram(np.array(dfs[1]["signal_length"]), bins=bin_range)
		
		if plot:
			
			fig = plt.figure(figsize=fig_size)
			plt.bar(range(len(bin_range)-1), height=counts, align="edge", width=0.9, color="r", alpha=0.5)
			plt.xlabel("Range of length of signals")
			plt.ylabel("Number of records")
			plt.suptitle("Signal: " + "/".join(dfs[0].loc[good_sig_name]["signal_id_names"]) + "\n" + "Number of records within ranges of length", fontsize=16)
			plt.xticks(range(len(bin_range)),bin_range)
			plt.show()
			
		return counts, bin_edges

def allsig_stats(opt="p", topn=None, plot=False, fig_size=(8,6), file_name="sigrecs", makedir="minedsets"):
	
	filename = goodstr(file_name)
	
	dfs = getsignaldf(file_name=filename, makedir=makedir)
	
	if opt == "p": column = "number_of_patients"; fname = "patients"
	elif opt == "r": column = "number_of_records"; fname = "records"
	else: print("Incorrect indicator for record type. Select 'p' for 'patients' or 'r' for 'records'."); return
	
	if "signal_id_names" not in list(dfs.columns):
		print("Information not available.")
		return
	
	else:
		
		sorted_sigs = dfs[column].sort_values(ascending=False)
		if type(topn) == int and topn > 0 and topn < len(sorted_sigs):
			sorted_sigs = sorted_sigs[:topn]
		
		if plot:
			
			fig = plt.figure(figsize=fig_size)
			plt.bar(np.arange(len(sorted_sigs)), sorted_sigs, width=0.9, align="center", color="purple", alpha=0.5)
			plt.xlabel("Signal")
			plt.ylabel("Number of " + fname + " containing the signal")
			plt.xticks(range(len(sorted_sigs)), list(dfs.ix[list(sorted_sigs.index)]["signal_id_names"].apply(lambda x: "/".join(x))), rotation="vertical")
			plt.suptitle("Signals statistics", fontsize=16)
			plt.show()
			
		return sorted_sigs

def rangelens_by_freq_stats(signal_name=None, signal=None, colnames=None, binsizes=None, timeunits=None, countthreshs=None, figsize=(8,6)):
	
	if signal is None or colnames is None or binsizes is None or timeunits is None or countthreshs is None:
		print("Invalid data, provide correct data.")
		return
	
	if (type(signal) != str and type(signal) != pd.core.frame.DataFrame) or type(colnames) != list or type(binsizes) != list or type(timeunits) != list or (type(timeunits) != list and not AinB(timeunits, ["samples", "seconds", "minutes", "hours"], "all")) or type(countthreshs) != list or type(figsize) != tuple:
		print("Invalid data, provide correct data.")
		return
	
	if type(signal) == str:
		data = getmetadf(signal)
	elif type(signal) == pd.core.frame.DataFrame:
		data = signal
		
	if data is None or (data is not None and not anyinAB(colnames, data.columns.values)):
		print("Invalid signal or column name.")
		return
	
	colors = ["magenta", "green", "orange"]
	
	colnames = list(set(colnames) & set(data.columns.values))

	freqs = sorted(set(data["sampling_frequency"].values))
	time_factor = lambda freq: {"samples": 1.0, "seconds": goodfreq(freq), "minutes": goodfreq(freq)*60.0, "hours": goodfreq(freq)*60.0*60.0}

	freq_bins = []
	freq_vals = []

	for i in range(len(freqs)):

		freq_records = data[data["sampling_frequency"] == freqs[i]]
		uniq_ele, uniq_ele_counts = np.unique(np.array(sorted(sum(list(np.array(freq_records[colnames].values.T).flatten()), []))), return_counts=True)
		
		max_lens = "Max count " + "(" + str(np.max(uniq_ele_counts)) + ")" + " lengths: " + ", ".join(map(lambda max_lens: str(max_lens), uniq_ele[np.argwhere(uniq_ele_counts == np.max(uniq_ele_counts)).flatten()]))
		
		threshed_uniq_ele = uniq_ele[np.argwhere(uniq_ele_counts > countthreshs[i]).flatten()] / time_factor(freqs[i])[timeunits[i]]
		threshed_uniq_ele_counts = uniq_ele_counts[np.argwhere(uniq_ele_counts > countthreshs[i]).flatten()]
		
		bins = range(1, int(np.max(threshed_uniq_ele) + binsizes[i]), binsizes[i])
		values = map(lambda indices: np.sum(threshed_uniq_ele_counts[np.argwhere(np.in1d(threshed_uniq_ele, indices)).flatten()]), np.split(threshed_uniq_ele, threshed_uniq_ele.searchsorted(bins)))
		bins.append(bins[-1]+binsizes[i])
		bins = list(np.array(bins)-1)

		fig = plt.figure(figsize=figsize)
		plt.fill_between(bins, values, step="pre", color=colors[i], alpha=0.5)
		plt.xlabel("Bins of lengths of " + " + ".join(colnames) + " \ " + timeunits[i])
		plt.ylabel("Number of occurrences")
		plt.suptitle("Signal: " + signal_name + "\n" + "Record count: " + str(len(freq_records.index.values)) + "\n\n" + "Frequency: " + str(freqs[i]) + " Hz" + "\n" + "Bin size: " + str(binsizes[i]) + "\n" + "Threshold: " + str(countthreshs[i]) + "\n" + max_lens, fontsize=12)
		plt.show()

		freq_bins.append(bins)
		freq_vals.append(values)

	return (freq_bins, freq_vals)