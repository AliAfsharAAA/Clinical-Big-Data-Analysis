"""
Simple stat generator for getting full population information on admittances per patient

This is built as a starting point and is meant to be easily extended so as not to repeat code

The master list is the RECORDS file provided that lists every matched record file
"""


import pandas as pd
from collections import defaultdict


#Need to have the path of the master list of all patient records
masterlist_path = './master_list'
masterlist = open(masterlist_path,'r')



patient_records = defaultdict(int)


#Iterate through masterlist,and then tally patient admittances
for record in masterlist:

    folder,patient,indiv_record = record.split('/')

    patient_records[patient] += 1


NUM_PATIENTS = len(patient_records)
#Using a dataframe we can easily generate some summary stats, as well as easily extend use cases with the dataframe
df = pd.DataFrame.from_dict(patient_records,orient = 'index')
print('# patients: ',NUM_PATIENTS)
print(df.describe(include = 'all'))
print('median: ',df.median(axis = 0))
print('mode: ',df.mode(axis = 0))
