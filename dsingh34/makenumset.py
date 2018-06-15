import os
import argparse
import re


database_dir = "/".join(os.popen("pwd").read().split()[0].split("/")[:-1])

data_path = database_dir + "/database/mimic3wdb/matched"
numeric_path = database_dir + "/database/mimic3wdb/matched_num"

parser = argparse.ArgumentParser()
parser.add_argument("-c", "--create", action="store_true", help="Create numeric dataset. Example: -c")
args = vars(parser.parse_args())


if args["create"]:

	os.system("cp " + data_path + "/RECORDS-numerics " + numeric_path)
	os.system("rsync -ap --prune-empty-dirs --include '*/' --include '*n.*' --exclude '*' " + data_path + "/* " + numeric_path) # removes the empty directories. If you want to keep empty directories, remove the --prune-empty-dirs flag.
	
	num_orig_hea = set([re.split("/|\.", filename)[-2] for filename in os.popen("find " + data_path + " -type f -name '*n.hea'").read().split()])
	num_made_hea = set([re.split("/|\.", filename)[-2] for filename in os.popen("find " + numeric_path + " -type f -name '*n.hea'").read().split()])
	num_orig_dat = set([re.split("/|\.", filename)[-2] for filename in os.popen("find " + data_path + " -type f -name '*n.dat'").read().split()])
	num_made_dat = set([re.split("/|\.", filename)[-2] for filename in os.popen("find " + numeric_path + " -type f -name '*n.dat'").read().split()])
	
	if num_orig_hea != num_made_hea or num_orig_dat != num_made_dat:
		if num_orig_hea != num_made_hea and num_orig_dat == num_made_dat: print("Error in copying numeric header files.")
		elif num_orig_hea == num_made_hea and num_orig_dat != num_made_dat: print("Error in copying numeric data files.")
		elif num_orig_hea != num_made_hea and num_orig_dat != num_made_dat: print("Error in copying both numeric header and data files.")
	else: print("Numeric files only database created successfully.")
