# Module to read data csv's created by mining scripts.


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

from .usefuns import goodstr, goodfreq, strL2intL, strLT2intLT, strLT2dtLT, AinB


def getsignaldf(file_name="sigrecs", makedir="minedsets"):
	
	good_name = goodstr(file_name)
	signalfile = os.popen("pwd").read()[:-1] + "/" + makedir + "/" + good_name + ".csv"
	
	int_cols = ["number_of_patients", "number_of_records"]
	str_L_cols = ["signal_id_names", "list_of_patients", "list_of_records"]
	
	all_cols = int_cols + str_L_cols
	
	if os.path.exists(signalfile):
		
		signalrecordsdf = pd.read_csv(signalfile)
		colnames = signalrecordsdf.columns.values
		
		for c in all_cols:
			
			if c in colnames:
		
				if c in int_cols:
					signalrecordsdf[c] = signalrecordsdf[c].astype(int)
		
				elif c in str_L_cols:
					signalrecordsdf[c] = signalrecordsdf[c].apply(lambda x: x.replace("[","").replace("]","").replace("'","").split(", "))
					
				else:
					print("Type column not available, create it.")
		
		if "signal_id" in colnames:
			signalrecordsdf.insert(0, "index", range(signalrecordsdf.shape[0]))
			signalrecordsdf.set_index("signal_id", inplace=True)
		
		return signalrecordsdf
	
	else:
		print("File does not exist/incorrect signal name.")
		return

def getmetadf(signal_name, makedir="minedsets"):
	
	good_sig_name = goodstr(signal_name)
	metadatafile = os.popen("pwd").read()[:-1] + "/" + makedir + "/" + good_sig_name + "_md.csv"
	
	dt_cols = ["datetime"]
	float_cols = ["sampling_frequency", "counter_frequency"]
	int_cols = ["signal_length", "true_signal_length", "non_segments", "nan_signal_length", "nan_segments", "big_nan_segments", "small_nan_segments", "small_art_segments", "big_art_segments", "proc_signal_segments", "number_of_present_signals"]
	int_L_cols = ["non_lengths", "nan_lengths", "big_nan_lengths", "small_nan_lengths", "small_art_lengths", "big_art_lengths", "proc_signal_lengths"]
	int_LT_cols = ["non_ranges", "nan_ranges", "start_nan_range", "end_nan_range", "big_nan_ranges", "small_nan_ranges", "small_art_ranges", "big_art_ranges", "proc_signal_ranges"]
	dt_LT_cols = ["non_timestamps", "nan_timestamps"]
	str_L_cols = ["present_signals"]
	
	all_cols = dt_cols + float_cols + int_cols + int_L_cols + int_LT_cols + dt_LT_cols + str_L_cols
	
	if os.path.exists(metadatafile):
		
		metadatafiledf = pd.read_csv(metadatafile)
		colnames = metadatafiledf.columns.values
		
		for c in all_cols:
			
			if c in colnames:
				
				if c in dt_cols:
					metadatafiledf[c] = metadatafiledf[c].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
		
				elif c in float_cols:
					metadatafiledf[c] = metadatafiledf[c].astype(float)
					metadatafiledf[c] = metadatafiledf[c].apply(lambda x: round(x, 5))
		
				elif c in int_cols:
					metadatafiledf[c] = metadatafiledf[c].astype(int)
		
				elif c in int_L_cols:
					metadatafiledf[c] = metadatafiledf[c].apply(lambda x: strL2intL(x))
		
				elif c in int_LT_cols:
					metadatafiledf[c] = metadatafiledf[c].apply(lambda x: strLT2intLT(x))
		
				elif c in dt_LT_cols:
					metadatafiledf[c] = metadatafiledf[c].apply(lambda x: strLT2dtLT(x))
		
				elif c in str_L_cols:
					metadatafiledf[c] = metadatafiledf[c].apply(lambda x: x.replace("[","").replace("]","").replace("'","").split(", "))
					
				else:
					print("Type column not available, create it.")
		
		if "record_id" in colnames:
			metadatafiledf.insert(0, "index", range(metadatafiledf.shape[0]))
			metadatafiledf.set_index("record_id", inplace=True)
		
		return metadatafiledf
	
	else:
		print("File does not exist/incorrect signal name.")
		return

def gettimedf(signal_name, proc=False, makedir="minedsets"):
	
	good_sig_name = goodstr(signal_name)
	
	if proc: timetype = "_pts"
	else: timetype = "_ts"
	
	timeseriesfile = os.popen("pwd").read()[:-1] + "/" + makedir + "/" + good_sig_name + timetype + ".csv"
	
	np_L_cols = ["sequence", "proc_sequence"]
	
	all_cols = np_L_cols
	
	if os.path.exists(timeseriesfile):
		
		timeseriesdf = pd.read_csv(timeseriesfile)
		colnames = timeseriesdf.columns.values
		
		for c in all_cols:
			
			if c in colnames:
				
				if c in np_L_cols:
					timeseriesdf[c] = timeseriesdf[c].apply(lambda x: np.fromstring(x.replace("[","").replace("]",""), sep=", "))
					
				else:
					print("Type column not available, create it.")
		
		if "record_id" in colnames:
			timeseriesdf.insert(0, "index", range(timeseriesdf.shape[0]))
			timeseriesdf.set_index("record_id", inplace=True)
		
		return timeseriesdf
	
	else:
		print("File does not exist/incorrect signal name.")
		return

def getsigdat(signalname=None, rts=False, proc=False, makedir="minedsets"):
	# Add processed sequence csv to read (signalname_pts.csv)
	goodsignalname = goodstr(signalname)
	data_path = os.popen("pwd").read()[:-1] + "/" + makedir
	
	timeseries_type = None
	if proc: timeseries_type = "_pts"
	else: timeseries_type = "_ts"
	
	if signalname is not None:
		
		nodictflag = False
		
		if rts:
			if os.path.exists(data_path + "/datadictwt.p"):
				datadict = pickle.load(open(data_path + "/datadictwt.p", "rb"))
				if "sigrecs" not in datadict.keys() or goodsignalname + "_md" not in datadict.keys() or goodsignalname + timeseries_type not in datadict.keys(): nodictflag = True
				else: return (datadict["sigrecs"], datadict[goodsignalname + "_md"], datadict[goodsignalname + timeseries_type])
			else: nodictflag = True
		else:
			if os.path.exists(data_path + "/datadict.p"):
				datadict = pickle.load(open(data_path + "/datadict.p", "rb"))
				if "sigrecs" not in datadict.keys() or goodsignalname + "_md" not in datadict.keys(): nodictflag = True
				else: return (datadict["sigrecs"], datadict[goodsignalname + "_md"])
			else: nodictflag = True
		
		if nodictflag:
			
			print("No dictionary available or signal not available in dictionary, reading directly from csv files.")
			
			if rts:
				if not os.path.exists(data_path + "/" + "sigrecs.csv") or not os.path.exists(data_path + "/" + goodsignalname + "_md.csv") or not os.path.exists(data_path + "/" + goodsignalname + timeseries_type + ".csv"):
					print("Csv files also not available. This means data for the signal is not available. Run data collection scripts (signalrecords.py and getsignaldata.py) for signal first.")
					return
				else: return (getsignaldf(), getmetadf(goodsignalname), gettimedf(goodsignalname, proc))
			else:
				if not os.path.exists(data_path + "/" + "sigrecs.csv") or not os.path.exists(data_path + "/" + goodsignalname + "_md.csv"):
					print("Csv files also not available. This means data for the signal is not available. Run data collection scripts (signalrecords.py and getsignaldata.py) for signal first.")
					return
				else: return (getsignaldf(), getmetadf(goodsignalname))
			
	else:
		print("No signal name provided.")
		return

def getalldata(rts=False, proc=False, makedir="minedsets"):
	
	data_path = os.popen("pwd").read()[:-1] + "/" + makedir
	
	dictavail = True
	
	if rts:
		if os.path.exists(data_path + "/datadictwt.p"):
			datadict = pickle.load(open(data_path + "/datadictwt.p", "rb"))
			return datadict
		else: dictavail = False
	else:
		if os.path.exists(data_path + "/datadict.p"):
			datadict = pickle.load(open(data_path + "/datadict.p", "rb"))
			return datadict
		else: dictavail = False
		
	if not dictavail:	
		
		print("Data dictionary not available, creating one...")
	
		files = [re.split("/|\.", f)[-2] for f in os.popen("ls " + data_path + "/*.csv").read().split()]
		
		if len(files) == 0:
			print("No data available to create dictionary.")
			return
		
		else:
			
			datadict = dict()
			for f in files:
				fullfile = data_path + "/" + f + ".csv"
				if os.path.exists(fullfile):
					if "_md" in f: datadict[f] = getmetadf(f.split("_")[0], makedir)
					elif "_ts" in f:
						if rts:
							if proc and "_pts" in f:
								datadict[f] = gettimedf(f.split("_")[0], makedir, proc)
							elif proc and "_pts" not in f:
								print("Proccessed timeseries data not available, using unprocessed timeseries data.")
								datadict[f] = gettimedf(f.split("_")[0], makedir)
							else:
								datadict[f] = gettimedf(f.split("_")[0], makedir)
								
					else: datadict[f] = getsignaldf(f, makedir)
			if rts:	
				with open(data_path + "/" + "datadictwt.p", "wb") as handle:
						pickle.dump(datadict, handle)
			else:
				with open(data_path + "/" + "datadict.p", "wb") as handle:
						pickle.dump(datadict, handle)
			
			return datadict