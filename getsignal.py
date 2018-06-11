import os
import csv
import argparse
from mpi4py import MPI
import wfdb


work_dir = os.popen("pwd").read().split()[0]
data_dir = "database/mimic3wdb/matched"
data_path = work_dir + "/" + data_dir
dirs = os.popen("ls " + data_path).read().split()


rank = MPI.COMM_WORLD.Get_rank()
size = MPI.COMM_WORLD.Get_size()

parser = argparse.ArgumentParser()
parser.add_argument("-sid", "--signal_id", nargs="+", default=[], help="Input (different versions of) the name of signal to be recorded. Example: -sid \"ABP Mean\" \"ABP MEAN\"")
parser.add_argument("-sf", "--sampfrom", type=int, default=0, help="Input the beginning of time series sequence. Example: -sf 100")
parser.add_argument("-st", "--sampto", type=int, default=-1, help="Input the end of time series sequence. -1 for end of sequence. Example: -st 1000")
parser.add_argument("-p", "--physical", action="store_true", help="Input whether signal is physical or digital. Example: -p")
parser.add_argument("-smd", "--savemetadata", action="store_true", help="Save or not save metadata in csv. Example: -smd")
parser.add_argument("-sts", "--savetimeseries", action="store_true", help="Save or not save timeseries data in csv. Example: -sts")
args = vars(parser.parse_args())


col_names = ["patient_id", "record_datetime", "location", "number_of_signals", "sampling_frequency", "counter_frequency", "signal_length", "signal_type", "signal_unit"]
signal_id = args["signal_id"]
if args["sampto"] == -1:
    end = "end"
else:
    end = args["sampto"]


filename1 = work_dir + "/processed_data/metadata/" + "-".join(signal_id).replace(" ", "_") + "_metadata" + ".csv"
filename2 = work_dir + "/processed_data/timeseries/" + "-".join(signal_id).replace(" ", "_") + "_timeseries" + ".csv"

if rank == 0:
        
    if args["savemetadata"]:
            
        os.system("mkdir -p " +  work_dir + "/processed_data/metadata")
            
        f1 = open(filename1, "a")
        writer = csv.writer(f1)
        writer.writerow(col_names)
        f1.close()
            
    if args["savetimeseries"]:
            
        os.system("mkdir -p " +  work_dir + "/processed_data/timeseries")
            
        col_names2 = col_names[:2]
        col_names2.append("time_series_sequence")
        
        f2 = open(filename2, "a")
        writer = csv.writer(f2)
        writer.writerow(col_names2)
        f2.close()


for dr in dirs:
    
    records_entry = list()
    records_data = list()
    
    if dirs.index(dr)%size != rank: continue
    
    else:
        
        level1 = data_path + "/" + dr
        level2 = os.popen("ls " + level1).read().split()
        for l2 in level2:
            level3 = os.popen("ls " + level1 + "/" + l2 + "/" + "p*n.hea").read().split()
            
            for l3 in level3:

                record = wfdb.io.rdrecord(

                record_name = l3.split(".")[0],
                sampfrom = args["sampfrom"],
                sampto = end,
                channels = "all",
                physical =  args["physical"],

                ).__dict__

                record_entry = list()
                record_data = list()
                signal_flag = False

                for sig_id in signal_id: 
                    if sig_id in record["sig_name"]:
                        signal_id_index = record["sig_name"].index(sig_id)
                        signal_flag = True
                        break

                if signal_flag:     # create records with data values for patient

                    record_entry.append(record["record_name"].split("-")[0])     # patient id for metadata csv
                    record_entry.append(str(record["base_date"]) + "_" + "-".join(str(record["base_time"]).split(":")))     # record datetime for metadata csv
                    if not record["comments"]: record_entry.append("n/a")     # location
                    else: record_entry.append(record["comments"][0].split(": ")[1])
                    record_entry.append(record["n_sig"])     # number of signals
                    record_entry.append(record["fs"])     # sampling frequency
                    if not record["counter_freq"]: record_entry.append("n/a")     # counter frequency
                    else: record_entry.append(record["counter_freq"])
                    record_entry.append(record["sig_len"])     # signal length
                    
                    record_data.append(record["record_name"].split("-")[0])     # patient id for time series csv
                    record_data.append(str(record["base_date"]) + "_" + "-".join(str(record["base_time"]).split(":")))     # record datetime for time series csv
                    
                    if record["p_signal"] is None:     # signal type
                        record_entry.append("digital")
                        record_data.extend(list(record["d_signal"].T[signal_id_index]))     # signal time series data
                    else:
                        record_entry.append("physical")
                        record_data.extend(list(record["p_signal"].T[signal_id_index]))     # signal time series data
                    record_entry.append(record["units"][signal_id_index])     # signal unit
                    
                    records_entry.append(record_entry)
                    records_data.append(record_data)
                    
        if args["savemetadata"]:
            f1 = open(filename1, "a")
            writer = csv.writer(f1)
            writer.writerows(records_entry)
            f1.close()
                
        if args["savetimeseries"]:                    
            f2 = open(filename2, "a")
            writer = csv.writer(f2)
            writer.writerows(records_data)
            f2.close()
