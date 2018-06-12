import os

database_dir = "/".join(os.popen("pwd").read().split()[0].split("/")[:-1])

data_path = database_dir + "/database/mimic3wdb/matched"
numeric_path = database_dir + "/database/mimic3wdb/matched_numeric"

os.system("rsync -ap --prune-empty-dirs --include '*/' --include '*n.*' --exclude '*' " + data_path + "/* " + numeric_path)

dhc = int(os.popen("find " + data_path + " -type f -name '*n.hea' | wc -l").read().split()[0])
ddc = int(os.popen("find " + data_path + " -type f -name '*n.dat' | wc -l").read().split()[0])
nhc = int(os.popen("find " + numeric_path + " -type f -name '*n.hea' | wc -l").read().split()[0])
ndc = int(os.popen("find " + numeric_path + " -type f -name '*n.dat' | wc -l").read().split()[0])

if dhc + ddc != nhc + ndc:
	if dhc != nhc and ddc == ndc: print("Error in copying numeric header files.")
	elif dhc == nhc and ddc != ndc: print("Error in copying numeric data files.")
	elif dhc != nhc and ddc != ndc: print("Error in copying both numeric header and data files.")
else: print("Numeric files copied successfully.")
