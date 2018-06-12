"""
This script is a tool to validate record files for derived directories, argument is integer percent of numeric records to check

"""

import os
import argparse
import random
import sys

parser = argparse.ArgumentParser()
parser.add_argument('percent',type = int,help = "Percent of data to randomly check")
arg = parser.parse_args()
percent = arg.percent


working_dir = os.popen("pwd").read().split()[0]

print("you selected {}% of data to check".format(percent))


#get record masterlist

#Local override uncomment if running locally
#masterlist_loc = 'master_list.txt'


masterlist_loc = 'metadata/master_list.txt'
masterlist_path = os.path.join(working_dir,masterlist_loc)

with open(masterlist_path,'r') as file:
    lines = [line.rstrip('\n') for line in file]


records = len(lines)

check_records = random.sample(range(1,records),int((percent/100)*records))

check_records_paths = [lines[i] for i in check_records]


#Database directory


database_path = os.path.join(working_dir,'database/mimic3wdb/matched_numeric')

for record in check_records_paths:
    record = record + '.hea'
    print(record)
    record_path = os.path.join(database_path,record)

    if os.path.isfile(record_path) and os.access(record_path, os.R_OK):
        continue
    else:
        sys.stderr.write('Error present in database, check file {}'.format(record))






