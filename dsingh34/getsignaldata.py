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

from numods.usefuns import goodstr, AinB, anyinAB, delnumsigname, argL2tupidx
from numods.proctime import givenanondict, giveartdict, classifynans, classifyarts, interpolate, nanbigarts


database_dir = "/".join(os.popen("pwd").read().split()[0].split("/")[:-1])
work_dir = os.popen("pwd").read().split()[0]
data_dir = "database/mimic3wdb/matched_num"
data_path = database_dir + "/" + data_dir
dirs = [dr.split("/")[-2] for dr in os.popen("ls -d " + data_path + "/*/").read().split()]


comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()


parser = argparse.ArgumentParser()
parser.add_argument("-sid", "--signal_id", default="", help="Input the name of signal to be recorded. Example: -sid \"ABP Mean\"")
parser.add_argument("-smd", "--savemetadata", action="store_true", help="Save or not save metadata in csv. Example: -smd")
parser.add_argument("-sts", "--savetimeseries", action="store_true", help="Save or not save timeseries data in csv. Example: -sts")
parser.add_argument("-spt", "--saveprocessedtimeseries", action="store_true", help="Save or not save processed timeseries data in csv. Example: -spt")
parser.add_argument("-sf", "--sampfrom", type=int, default=0, help="Input the beginning of time series sequence. Example: -sf 100")
parser.add_argument("-st", "--sampto", type=int, default=-1, help="Input the end of time series sequence. -1 for end of sequence. Example: -st 1000")
parser.add_argument("-rn", "--remove_nans", default="none", help="Select from 'start', 'end', 'all' or 'none' to remove nans from time series sequence. Example: -rn \"all\"")
parser.add_argument("-dir", "--directory", default="minedsets", help="Input the subdirectory path from current directory to save files. Example: -dir \"savedfiles\"")
args = vars(parser.parse_args())


signal_id = args["signal_id"]
if args["sampto"] == -1:
  end = "end"
else:
  end = args["sampto"]
makedir = args["directory"]
if makedir == "": makedir = "minedsets"


col_names1 = ["record_id",
              "patient_id",
              "datetime",
              "type",
              "unit",
              "location",
              "sampling_frequency",
              "counter_frequency",
              "signal_length",
              "true_signal_length",
              "non_segments",
              "non_lengths",
              "non_ranges",
              "non_timestamps",
              "nan_signal_length",
              "nan_segments",
              "nan_lengths",
              "nan_ranges",
              "nan_timestamps",
              "start_nan_range",
              "end_nan_range",
              "big_nan_segments",
              "big_nan_lengths",
              "big_nan_ranges",
              "small_nan_segments",
              "small_nan_lengths",
              "small_nan_ranges",
              "small_art_segments",
              "small_art_lengths",
              "small_art_ranges",
              "big_art_segments",
              "big_art_lengths",
              "big_art_ranges",
              "proc_signal_segments",
              "proc_signal_lengths",
              "proc_signal_ranges",
              "number_of_present_signals",
              "present_signals"]

col_names2 = ["record_id", "sequence"]

col_names3 = ["record_id", "proc_sequence"]

signaldict = pickle.load(open("signaldict.p", "rb"))

goodsignalname = goodstr(signal_id)

if goodsignalname not in signaldict.keys():
  print("Invalid signal name (consult \"signaldict\" for valid signals).")
  sys.exit()

if rank == 0 and os.path.exists(work_dir + "/" + makedir):
  if len(os.popen("find " + work_dir + "/" + makedir + " -type f -name '" + "temp_" + goodsignalname +  "*.pkl'").read().split()) != 0:
    os.system("rm " + work_dir + "/" + makedir + "/" + "temp_" + goodsignalname + "*.pkl")

records_entry = list()
records_data = list()
records_proc_data = list()

proc_time = time.time()

for dr in dirs:
  
  if dirs.index(dr)%size != rank: continue
  
  else:
    
    if args["savemetadata"] or args["savetimeseries"]:
      
      level1 = data_path + "/" + dr
      level2 = os.popen("ls " + level1).read().split()
      
      for l2 in level2:
        
        level3 = os.popen("ls " + level1 + "/" + l2 + "/" + "p*n.hea").read().split()
        
        for l3 in level3:
          
          readfileflag = False
          
          try:
            
            record = wfdb.io.rdrecord(
            
              record_name = l3.split(".")[0],
              sampfrom = args["sampfrom"],
              sampto = end,
              channels = "all"
              
                                      ).__dict__
                                      
            readfileflag = True
            
          except ValueError:
            
            print("\nError in opening file:" + "\t" + dr + "/" + l2 + "/" + l3.split("/")[-1].split(".")[0])
            
            readfileflag = False
            
          if readfileflag and anyinAB(signaldict[goodsignalname], record["sig_name"]):     # create records with data values for patient
            
            signal_id_index = map(goodstr, record["sig_name"]).index(goodsignalname)
            
            present_signals = map(goodstr, delnumsigname(record["sig_name"]))
                        
            timesequence = None
            if record["p_signal"] is None:
              timesequence = record["d_signal"].T[signal_id_index]    # get the signal time series data and process for nans
              signal_type = "d_signal"
            else:
              timesequence = record["p_signal"].T[signal_id_index]
              signal_type = "p_signal"
            
            original_timesequence = np.copy(timesequence)
            
            dt = str(record["base_date"]) + " " + str(record["base_time"]).split(".")[0]
            dt = datetime.datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
            
            timestr = False        
            
            nanondict = givenanondict(timesequence, record["fs"], dt, timestr)
            
            classifiednans = classifynans(nanondict)
            
            if not timestr:
              for timestamp_type in ["nan_timestamps", "non_timestamps"]:
                nanondict[timestamp_type] = [(t[0].strftime("%Y-%m-%d %H:%M:%S"), t[1].strftime("%Y-%m-%d %H:%M:%S")) for t in nanondict[timestamp_type]]
            
            removed_smallnans = interpolate(classifiednans["mid_nan_not_hour_ranges"], timesequence)    # signal time series data with nans removed

            rem_smallnan_pipe = lambda ts, fs, dt: interpolate(classifynans(givenanondict(ts, fs, dt))["mid_nan_not_hour_ranges"], ts)

            sequence2d = timesequence
            
            if goodsignalname in ["abpsys", "abpdias"] and len(record["sig_name"]) > 1 and AinB(["abpsys", "abpdias"], present_signals, "all"):
              if goodsignalname is "abpsys":
                abpsys_remd_smallnans = rem_smallnan_pipe(timesequence, record["fs"], dt)
                abpdias_seq = record[signal_type].T[map(goodstr,record["sig_name"]).index("abpdias")]
                abpdias_remd_smallnans = rem_smallnan_pipe(abpdias_seq, record["fs"], dt)
                sequence2d = np.vstack((abpsys_remd_smallnans, abpdias_remd_smallnans))
              
              elif goodsignalname is "abpdias":
                abpsys_seq = record[signal_type].T[map(goodstr,record["sig_name"]).index("abpsys")]
                abpsys_remd_smallnans = rem_smallnan_pipe(abpsys_seq, record["fs"], dt)
                abpdias_remd_smallnans = rem_smallnan_pipe(timesequence, record["fs"], dt)
                sequence2d = np.vstack((abpsys_remd_smallnans, abpdias_remd_smallnans))
             
            classifiedarts = classifyarts(goodsignalname, sequence2d, record["fs"])
            
            artdict = giveartdict(classifiedarts, timesequence, record["fs"], dt)
            
            removed_smallnans_arts = interpolate(artdict["small_art_ranges"], removed_smallnans)    # signal time series data with small nans and artifacts removed
            
            removed_smallnans_filled_bigarts_arts = nanbigarts(artdict["big_art_ranges"], removed_smallnans_arts)
            
            proc_dict = argL2tupidx(removed_smallnans_filled_bigarts_arts, "notnan")
            
            if args["savemetadata"]:
                            
              record_entry = list()
                            
              record_entry.append(record["record_name"])     # record id for metadata csv
              record_entry.append(record["record_name"].split("-")[0])     # patient id for metadata csv
              record_entry.append(str(record["base_date"]) + " " + str(record["base_time"]).split(".")[0])     # record datetime for metadata csv
              if record["p_signal"] is None: record_entry.append("digital")     # signal type
              else: record_entry.append("physical")
              record_entry.append(record["units"][signal_id_index])     # signal unit
              if not record["comments"]: record_entry.append(np.nan)     # location
              else: record_entry.append(record["comments"][0].split(": ")[1])
              record_entry.append(record["fs"])     # sampling frequency
              record_entry.append(record["counter_freq"])
              record_entry.append(record["sig_len"])     # signal length
                            
              record_entry.append(sum(nanondict["non_lengths"]))     # true signal length
              record_entry.append(nanondict["non_segments"])     # number of non segments
              record_entry.append(nanondict["non_lengths"])     # list of lengths of non segments
              record_entry.append(nanondict["non_ranges"])     # list of ranges of non segments
              record_entry.append(nanondict["non_timestamps"])     # list of timestamps of ranges of non segments
              record_entry.append(sum(nanondict["nan_lengths"]))     # all nans length
              record_entry.append(nanondict["nan_segments"])     # number of nan segments
              record_entry.append(nanondict["nan_lengths"])     # list of lengths of nan segments
              record_entry.append(nanondict["nan_ranges"])     # list of ranges of non segments
              record_entry.append(nanondict["nan_timestamps"])     # list of timestamps of ranges of non segments
              record_entry.append(classifiednans["start_nan"])     # list of start nan
              record_entry.append(classifiednans["end_nan"])     # list of end nan
              record_entry.append(classifiednans["mid_nan_hour_segments"])     # number of nan ranges of length longer than an hour
              record_entry.append(classifiednans["mid_nan_hour_lengths"])     # list of lengths of nan ranges of length longer than an hour
              record_entry.append(classifiednans["mid_nan_hour_ranges"])     # list of nan ranges of length longer than an hour
              record_entry.append(classifiednans["mid_nan_not_hour_segments"])     # number of nan ranges of length shorter than an hour
              record_entry.append(classifiednans["mid_nan_not_hour_lengths"])     # list of lengths of nan ranges of length shorter than an hour
              record_entry.append(classifiednans["mid_nan_not_hour_ranges"])     # list of nan ranges of length shorter than an hour
              record_entry.append(artdict["small_art_segments"])     # number of small artifact ranges
              record_entry.append(artdict["small_art_lengths"])     # list of small artifact range lengths
              record_entry.append(artdict["small_art_ranges"])     # list of small artifact ranges
              record_entry.append(artdict["big_art_segments"])     # number of big artifact ranges
              record_entry.append(artdict["big_art_lengths"])     # list of big artifact range lengths
              record_entry.append(artdict["big_art_ranges"])     # list of big artifact ranges
              record_entry.append(proc_dict["segments"])     # number of processed signal ranges
              record_entry.append(proc_dict["lengths"])     # processed signal lengths
              record_entry.append(proc_dict["ranges"])     # processed signal ranges
              record_entry.append(len(delnumsigname(record["sig_name"])))     # number of present signals
              record_entry.append(present_signals)     # list of present signals
                                
              records_entry.append(record_entry)
                            
            if args["savetimeseries"]:
                             
              record_data = list()
                                
              record_data.append(record["record_name"])     # record id for time series csv
              record_data.append(list(original_timesequence))    # signal time series data
              
              records_data.append(record_data)
              
            if args["saveprocessedtimeseries"]:
              
              record_proc_data = list()
                                
              record_proc_data.append(record["record_name"])     # record id for processed time series csv
              record_proc_data.append(list(removed_smallnans_filled_bigarts_arts))    # signal processed time series data
              
              records_proc_data.append(record_proc_data)
          
      if args["savemetadata"]:
        
        if not os.path.exists(work_dir + "/" + makedir):
          os.system("mkdir -p " +  work_dir + "/" + makedir)
          
        handle = open(work_dir + "/" + makedir + "/" + "temp_" + goodsignalname + "_" + dr + "_md.pkl", "wb")
        pickle.dump(records_entry, handle)
        handle.close()
 
      if args["savetimeseries"]:
        
        if not os.path.exists(work_dir + "/" + makedir):
          os.system("mkdir -p " +  work_dir + "/" + makedir)
          
        handle = open(work_dir + "/" + makedir + "/" + "temp_" + goodsignalname + "_" + dr + "_ts.pkl", "wb")
        pickle.dump(records_data, handle)
        handle.close()

      if args["saveprocessedtimeseries"]:
  
        if not os.path.exists(work_dir + "/" + makedir):
          os.system("mkdir -p " +  work_dir + "/" + makedir)
    
        handle = open(work_dir + "/" + makedir + "/" + "temp_" + goodsignalname + "_" + dr + "_pts.pkl", "wb")
        pickle.dump(records_proc_data, handle)
        handle.close()

proc_time = round((time.time() - proc_time) / 60.0, 2)
dirs_proc_time = comm.gather(proc_time, root=0)

if rank == 0:
  
  dir_time_match_flag = False
  if len(dirs) == len(dirs_proc_time):
    dir_time_match_flag = True
    dr_dirs_proc_time =  zip(dirs, dirs_proc_time)
    print("The processing time for each directory is:\nDIR\tTIME (minutes)")
    for t in dr_dirs_proc_time:
      print(str(t[0]) + "\t" + str(t[1]))
    print("Total processing time is " + str(np.mean(dirs_proc_time)) + " minutes.")
  else:
    print("Directories length not matching to list of measured time for each process list.")
  
  if args["savemetadata"]:
    
    filename1 = work_dir + "/" + makedir + "/" + goodsignalname + "_md" + ".csv"

    if os.path.exists(filename1): os.system("rm " + filename1)
    
    while len(os.popen("find " + work_dir + "/" + makedir + " -type f -name 'temp_" + goodsignalname + "_" + "*" + "_md.pkl'").read().split()) != len(dirs): time.sleep(5)
    


  
#    f = open("minedsets/hr_md.txt","ab")
#    pickle.dump(col_names1, f)
#    for dr in dirs:
#      handle = open(work_dir + "/" + makedir + "/" + "temp_" + goodsignalname + "_" + dr + "_md.pkl", "rb")
#      records_entry = pickle.load(handle)
#      handle.close()
#      for h in records_entry:
#        pickle.dump(h, f)
#    f.close()
#    os.system("rm " + work_dir + "/" + makedir + "/" + "temp_" + goodsignalname + "*_md.pkl") 




    records_entry = list()
    with open(filename1, "a") as f:
      writer = csv.writer(f)
      writer.writerow(col_names1)
      for dr in dirs:
        handle = open(work_dir + "/" + makedir + "/" + "temp_" + goodsignalname + "_" + dr + "_md.pkl", "rb")
        records_entry = pickle.load(handle)
        handle.close()
        writer.writerows(records_entry)
    os.system("rm " + work_dir + "/" + makedir + "/" + "temp_" + goodsignalname + "*_md.pkl")    
    
#    dr_dirs_md_time = None
#    if dir_time_match_flag:
#      dr_dirs_md_time = zip(dirs, dirs_md_time)
#      print("The metadata saving time for each directory is:\nDIR\tTIME (minutes)")
#      for t in dr_dirs_md_time:
#        print t[0], "\t", t[1]
#    print("Total metadata saving time is " + str(sum(dirs_md_time)) + " minutes.")
    
    if os.path.exists(filename1):
      print("\nMetadata csv file saved successfully.")
    else: print("\nError in saving metadata csv file.")
            
  if args["savetimeseries"]:
    
    filename2 = work_dir + "/" + makedir + "/" + goodsignalname + "_ts" + ".csv"
        
    if os.path.exists(filename2): os.system("rm " + filename2)
    
    while len(os.popen("find " + work_dir + "/" + makedir + " -type f -name 'temp_" + goodsignalname + "_" + "*" + "_ts.pkl'").read().split()) != len(dirs): time.sleep(5)
        
    records_data = list()
    with open(filename2, "a") as f:
      writer = csv.writer(f)
      writer.writerow(col_names2)
      for dr in dirs:
        handle = open(work_dir + "/" + makedir + "/" + "temp_" + goodsignalname + "_" + dr + "_ts.pkl", "rb")
        records_data = pickle.load(handle)
        handle.close()
        writer.writerows(records_data)
    os.system("rm " + work_dir + "/" + makedir + "/" + "temp_" + goodsignalname + "*_ts.pkl")
    
#    dr_dirs_ts_time = None
#    if dir_time_match_flag:
#      dr_dirs_ts_time = zip(dirs, dirs_ts_time)
#      print("The timeseries saving time for each directory is:\nDIR\tTIME (minutes)")
#      for t in dr_dirs_ts_time:
#        print t[0], "\t", t[1]
#    print("Total timeseries saving time is " + str(sum(dirs_ts_time)) + " minutes.")
        
    if os.path.exists(filename2):
      print("\nTimeseries csv file saved successfully.")
    else: print("\nError in saving timeseries csv file.")

  if args["saveprocessedtimeseries"]:
  
    filename3 = work_dir + "/" + makedir + "/" + goodsignalname + "_pts" + ".csv"
      
    if os.path.exists(filename3): os.system("rm " + filename3)
  
    while len(os.popen("find " + work_dir + "/" + makedir + " -type f -name 'temp_" + goodsignalname + "_" + "*" + "_pts.pkl'").read().split()) != len(dirs): time.sleep(5)
  
    records_proc_data = list()
    with open(filename3, "a") as f:
      writer = csv.writer(f)
      writer.writerow(col_names3)
      for dr in dirs:
        handle = open(work_dir + "/" + makedir + "/" + "temp_" + goodsignalname + "_" + dr + "_pts.pkl", "rb")
        records_proc_data = pickle.load(handle)
        handle.close()
        writer.writerows(records_proc_data)
    os.system("rm " + work_dir + "/" + makedir + "/" + "temp_" + goodsignalname + "*_pts.pkl")
    
#    dr_dirs_pts_time = None
#    if dir_time_match_flag:
#      dr_dirs_pts_time = zip(dirs, dirs_pts_time)
#      print("The processed timeseries saving time for each directory is:\nDIR\tTIME (minutes)")
#      for t in dr_dirs_pts_time:
#        print t[0], "\t", t[1]
#    print("Total processed timeseries saving time is " + str(sum(dirs_pts_time)) + " minutes.")
      
    if os.path.exists(filename3):
      print("\nProcessed timeseries csv file saved successfully.")
    else: print("\nError in saving processed timeseries csv file.")