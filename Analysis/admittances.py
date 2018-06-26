"""
Simple stat generator for getting full population information on admittances per patient

This is built as a starting point and is meant to be easily extended so as not to repeat code

The master list is the RECORDS file provided that lists every matched record file
"""


import pandas as pd
from collections import defaultdict
import os

print(os.listdir())
#Need to have the path of the master list of all patient records
masterlist_path = './master_list.txt'
masterlist = open(masterlist_path,'r')



patient_records = defaultdict(int)

total_records = 0
#Iterate through masterlist,and then tally patient admittances
for record in masterlist:

    folder,patient,indiv_record = record.split('/')

    patient_records[patient] += 1
    total_records += 1



for patient in patient_records:
    if patient_records[patient] == 56:
        print(patient)
NUM_PATIENTS = len(patient_records)
#Using a dataframe we can easily generate some summary stats, as well as easily extend use cases with the dataframe
df = pd.DataFrame.from_dict(patient_records,orient = 'index')
print('# patients: ',NUM_PATIENTS)
print(df.describe(include = 'all'))
print('median: ',df.median(axis = 0))
print('mode: ',df.mode(axis = 0))

print('Total records: ',total_records)