import os
import csv
import argparse
from mpi4py import MPI
import wfdb
from collections import defaultdict
from collections import Counter
import operator


def joindicts(dict1, dict2, op=operator.add):
    return dict(dict1.items() + dict2.items() + [(key, op(dict1[key], dict2[key])) for key in set(dict1) & set(dict2)])


database_dir = "/".join(os.popen("pwd").read().split()[0].split("/")[:-1])
work_dir = os.popen("pwd").read().split()[0]
data_dir = "database/mimic3wdb/matched_numeric"
data_path = database_dir + "/" + data_dir
dirs = [dr.split("/")[-2] for dr in os.popen("ls -d " + data_path + "/*/").read().split()]


comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

parser = argparse.ArgumentParser()
parser.add_argument("-s", "--save", action="store_true", help="Save or not save data in csv. Example: -s")
args = vars(parser.parse_args())


col_names = ["signal_id", "number_of_patients", "number_of_records", "list_of_records"]
filename = work_dir + "/processed_data/signal_records.csv"

if rank == 0:
    
    counters = list()
    ids = list()
    
    if args["save"]:
        
        if os.path.exists(work_dir + "/processed_data"):
            if os.path.exists(filename):
                os.system("rm " + filename)
        else: os.system("mkdir -p " +  work_dir + "/processed_data")
        
        f = open(filename, "a")
        writer = csv.writer(f)
        writer.writerow(col_names)
        f.close()

temp_record_signal_counts = Counter()
temp_record_signal_ids = defaultdict(list)

for dr in dirs:
    
    if dirs.index(dr)%size != rank: continue
    
    else:
    
        level1 = data_path + "/" + dr
        level2 = os.popen("ls " + level1).read().split()
        for l2 in level2:
            level3 = os.popen("ls " + level1 + "/" + l2 + "/" + "p*n.hea").read().split()

            for l3 in level3:

                record = wfdb.io.rdrecord(

                record_name = l3.split(".")[0],
                sampfrom = 0,
                sampto = "end",
                channels = "all",
                physical =  True,

                ).__dict__
                
                temp_record_signal_counts.update(record["sig_name"])
                for signame in record["sig_name"]:    
                    temp_record_signal_ids[signame].append( record["record_name"].split("-")[0] + "__" + str(record["base_date"]) + "_" + "-".join(str(record["base_time"]).split(":")) )
                    
temp_record_signal_counts = comm.gather(temp_record_signal_counts, root = 0)
temp_record_signal_ids = comm.gather(temp_record_signal_ids, root = 0)

if rank == 0:
    counter_count = 0
    defaultdict_count = 0
    for temp_record_signal_count in temp_record_signal_counts:
        if temp_record_signal_count != Counter(): counter_count += 1
    for temp_record_signal_id in temp_record_signal_ids:
        if temp_record_signal_id != defaultdict(list): defaultdict_count += 1
    assert counter_count == len(dirs) and defaultdict_count == len(dirs)
else: assert temp_record_signal_counts is None and temp_record_signal_ids is None

if rank == 0:
    
    record_signal_counts = temp_record_signal_counts[0]
    for counter in temp_record_signal_counts[1:]:
            record_signal_counts = record_signal_counts + counter

    record_signal_id = temp_record_signal_ids[0]
    for recdid in temp_record_signal_ids[1:]:
        record_signal_id = joindicts(record_signal_id, recdid)

    signal_count_id = list()
    pc = lambda x: x.split("__")[0]  
    for key in record_signal_counts.keys():
        patient_count = len(set([pc(l) for l in record_signal_id[key]]))
        signal_record = [key, patient_count, record_signal_counts[key]]
        signal_record.extend(record_signal_id[key])
        signal_count_id.append(signal_record)              

    if args["save"]:
        f = open(filename, "a")
        writer = csv.writer(f)
        writer.writerows(signal_count_id)
        f.close()
    
        if os.path.exists(filename):
            print("Csv data file saved successfully.")
        else: print("Error in saving csv file.")
