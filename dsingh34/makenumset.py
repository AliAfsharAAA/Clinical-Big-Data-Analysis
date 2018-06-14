import os
import argparse


database_dir = "/".join(os.popen("pwd").read().split()[0].split("/")[:-1])

data_path = database_dir + "/database/mimic3wdb/matched"
numeric_path = database_dir + "/database/mimic3wdb/matched_num"

parser = argparse.ArgumentParser()
parser.add_argument("-c", "--create", action="store_true", help="Create numeric dataset. Example: -c")
args = vars(parser.parse_args())


if args["create"]:

	os.system("rsync -ap --prune-empty-dirs --include '*/' --include '*n.*' --exclude '*' " + data_path + "/* " + numeric_path) # removes the empty directories. If you want to keep empty directories, remove the --prune-empty-dirs flag.
	os.system("cp " + data_path + "/RECORDS-numerics " + numeric_path)
	
	dhc = len(os.popen("find " + data_path + " -type f -name '*n.hea'").read().split())
	ddc = len(os.popen("find " + data_path + " -type f -name '*n.dat'").read().split())
	nhc = len(os.popen("find " + numeric_path + " -type f -name '*n.hea'").read().split())
	ndc = len(os.popen("find " + numeric_path + " -type f -name '*n.dat'").read().split())
	
	if dhc + ddc != nhc + ndc:
		if dhc != nhc and ddc == ndc: print("Error in copying numeric header files.")
		elif dhc == nhc and ddc != ndc: print("Error in copying numeric data files.")
		elif dhc != nhc and ddc != ndc: print("Error in copying both numeric header and data files.")
	else: print("Numeric files only database created successfully.")
