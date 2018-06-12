#!/usr/bin/python
import os
import shutil



import argparse
parser = argparse.ArgumentParser()
parser.add_argument('folder',type = str,help = "folder to copy")
arg = parser.parse_args()
folder_arg = arg.folder

folder = folder_arg
working_dir = os.popen("pwd").read().split()[0]

copy_from_path = os.path.join(working_dir,'database/mimic3wdb/matched')

copy_to_path = os.path.join(working_dir,os.path.join('database/mimic3wdb/matched_numeric',folder_arg))



folder_path = os.path.join(copy_from_path,folder)

patient_records = os.listdir(folder_path)

for patient_record in patient_records:

    from_location = os.path.join(folder_path,patient_record)

    records = [file for file in os.listdir(from_location) if file.endswith('n.hea') or file.endswith('n.dat')]


    if records != []:

        destination_path = os.path.join(copy_to_path,patient_record)

        os.makedirs(destination_path, exists_ok=True)


        for record in records:
            full_file_name = os.path.join(from_location,record)
            shutil.copy(full_file_name,)

