import os

import argparse
import pandas as pd

#Get a list of the directories and paths of the data folders
database_dir = "/".join(os.popen("pwd").read().split()[0].split("/")[:-1])
work_dir = os.popen("pwd").read().split()[0]
clinical_dir = "database/mimic3wdb/clinical"
data_path = os.path.join(database_dir,clinical_dir)

clinical_matched_dir = os.path.join(database_dir,'matched_clinical')


parser = argparse.ArgumentParser()
parser.add_argument('file',type = str,help = "CSV file to transfer")
arg = parser.parse_args()
filename = arg.file

print(filename)
mlist = 'metadata/master_list.txt'
mlistpath = os.path.join(database_dir,mlist)
mfile = open(mlistpath, 'r')

ids = []
for line in mfile:
    _, ID, _ = line.split('/')
    ID = int(ID[2:])
    ids.append(ID)


ids = list(set(ids))
ids.sort()



df = pd.read_csv(os.path.join(data_path,filename))
df.sort_values('SUBJECT_ID')

newdf = df.loc[df['SUBJECT_ID'].isin(ids)]




outputPath = os.path.join(clinical_matched_dir,filename)
newdf.to_csv(path_or_buf=outputPath,index = False)