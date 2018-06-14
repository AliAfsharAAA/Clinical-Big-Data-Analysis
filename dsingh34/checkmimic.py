import os
import argparse
import re


database_dir = "/".join(os.popen("pwd").read().split()[0].split("/")[:-1])

data_path = database_dir + "/database/mimic3wdb/matched"
numeric_path = database_dir + "/database/mimic3wdb/matched_num"

parser = argparse.ArgumentParser()
parser.add_argument("-c", "--check", action="store_true", help="Check whether mimic-iii matched waveform dataset has been copied correctly or not. Example: -c")
args = vars(parser.parse_args())


if args["check"]:
	
	wf_records_list = [filename.split("/")[-1] for filename in open(data_path + "/RECORDS-waveforms", "r+").read().split("\n")[:-1]]
	num_records_list = [filename.split("/")[-1] for filename in open(data_path + "/RECORDS-numerics", "r+").read().split("\n")[:-1]]
	
	wf_downloaded_list = [re.split("/|\.", filename)[-2] for filename in os.popen("find " + data_path + " -type f -name '*[-]*[[:digit:]].hea'").read().split()]
	num_downloaded_list = [re.split("/|\.", filename)[-2] for filename in os.popen("find " + data_path + " -type f -name '*n.hea'").read().split()]
	
	wf_flag = False
	num_flag = False
	if set(wf_records_list) == set(wf_downloaded_list):
		print("Waveform records downloaded correctly.")
		wf_flag = True
	else: print("Error in downloading waveform records.")
	if set(num_records_list) == set(num_downloaded_list):
		print("Numeric records downloaded correctly.")
		num_flag = True
	else: print("Error in downloading numeric records.")
	if wf_flag and num_flag: print("Mimic-III waveform matched subset downloaded correctly (according to the provided records lists.)")
