import os
import csv
import argparse
from collections import defaultdict
from collections import Counter
import wfdb


work_dir = os.popen("pwd").read().split()[0]
data_dir = "database/mimic3wdb/matched"
data_path = work_dir + "/" + data_dir
dirs = os.popen("ls " + data_path).read().split()


parser = argparse.ArgumentParser()
parser.add_argument("-s", "--save", type=bool, default=False, help="Save or not save data in csv. Example: -s 1 OR -s False")
args = vars(parser.parse_args())


col_names = ["signal_id", "number_of_patients", "number_of_records", "list_of_records"]
filename = work_dir + "/processed_data/signal_records.csv"


if args["save"]:

    os.system("mkdir -p " +  work_dir + "/processed_data")
    
    f = open(filename, "a")
    writer = csv.writer(f)
    writer.writerow(col_names)
    f.close()


record_signal_counts = Counter()
record_signal_id = defaultdict(list)

for dr in dirs:
    
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
            
            record_signal_counts.update(record["sig_name"])
            for signame in record["sig_name"]:    
                record_signal_id[signame].append( record["record_name"].split("-")[0] + "__" + str(record["base_date"]) + "_" + "-".join(str(record["base_time"]).split(":")) )

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