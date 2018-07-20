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

from numods.usefuns import goodstr, AinB, delnumsigname, joindicts


database_dir = "/".join(os.popen("pwd").read().split()[0].split("/")[:-1])
work_dir = os.popen("pwd").read().split()[0]
data_dir = "database/mimic3wdb/matched_num"
data_path = database_dir + "/" + data_dir
dirs = [dr.split("/")[-2] for dr in os.popen("ls -d " + data_path + "/*/").read().split()]


comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()


parser = argparse.ArgumentParser()
parser.add_argument("-s", "--save", action="store_true", help="Save or not save data in csv. Example: -s")
parser.add_argument("-dir", "--directory", default="minedsets", help="Input the subdirectory path from current directory to save files. Example: -dir \"savedfiles\"")
args = vars(parser.parse_args())


makedir = args["directory"]
if makedir == "": makedir = "minedsets"	


col_names = ["signal_id", "signal_id_names", "number_of_patients", "number_of_records", "list_of_patients", "list_of_records"]

temp_record_signal_counts = Counter()
temp_record_signal_records = defaultdict(list)
temp_record_signal_patients = defaultdict(list)

signaldict = pickle.load(open("signaldict.p", "rb"))

for dr in dirs:
	
	if dirs.index(dr)%size != rank: continue
	
	else:
	
		level1 = data_path + "/" + dr
		level2 = os.popen("ls " + level1).read().split()
		for l2 in level2:
			level3 = os.popen("ls " + level1 + "/" + l2 + "/" + "p*n.hea").read().split()

			for l3 in level3:
				
				readfileflag = False
				
				try:
					record = wfdb.io.rdrecord(

					record_name = l3.split(".")[0],
					sampfrom = 0,
					sampto = "end",
					channels = "all"

					).__dict__
					
					readfileflag = True
					
				except ValueError:
					
					print("\nError in opening file:" + "\t" + dr + "/" + l2 + "/" + l3.split("/")[-1].split(".")[0])
					
					readfileflag = False
					
				if readfileflag:
					
					temp_record_signal_counts.update(list(map(goodstr, record["sig_name"])))
					for signame in record["sig_name"]:    
						temp_record_signal_records[goodstr(signame)].append( record["record_name"].split("-")[0] + "-" + str(record["base_date"]) + "-" + "-".join(str(record["base_time"]).split(":")[:-1]) + "n" )
						temp_record_signal_patients[goodstr(signame)].append( record["record_name"].split("-")[0] )
					
temp_record_signal_counts = comm.gather(temp_record_signal_counts, root = 0)
temp_record_signal_records = comm.gather(temp_record_signal_records, root = 0)
temp_record_signal_patients = comm.gather(temp_record_signal_patients, root = 0)


if rank == 0:
	
	record_signal_counts = temp_record_signal_counts[0]
	for counter in temp_record_signal_counts[1:]:
			record_signal_counts = record_signal_counts + counter

	record_signal_records = temp_record_signal_records[0]
	for recdid in temp_record_signal_records[1:]:
		record_signal_records = joindicts(record_signal_records, recdid)
		
	record_signal_patients = temp_record_signal_patients[0]
	for ptid in temp_record_signal_patients[1:]:
		record_signal_patients = joindicts(record_signal_patients, ptid)

	signal_count_id = list()
	for key in record_signal_counts.keys():
		if key in signaldict.keys():
			signal_count_id.append([key, signaldict[key], len(set(record_signal_patients[key])), record_signal_counts[key], list(set(record_signal_patients[key])), record_signal_records[key]])
	
	signalrecordsdf = pd.DataFrame(signal_count_id, columns=col_names)
	
	signalrecordsdf = signalrecordsdf.sort_values("signal_id") # sort by signal id's
	signalrecordsdf = signalrecordsdf[signalrecordsdf["signal_id"].apply(delnumsigname)] # removes signal id's that do not have any letters in them i.e. that do not make sense (eg: [63456])

	if args["save"]:
		
		filename = work_dir + "/" + makedir + "/" + "sigrecs.csv"
		
		if os.path.exists(work_dir + "/" + makedir):
			if os.path.exists(filename):
				os.system("rm " + filename)
		else: os.system("mkdir -p " +  work_dir + "/" + makedir)
		
		signalrecordsdf.to_csv(filename, sep=",", index=False)
	
		if os.path.exists(filename):
			print("Csv data file saved successfully.")
		else: print("Error in saving csv file.")