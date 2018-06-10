#!/usr/bin/python

import os

working_dir = os.popen("pwd").read().split()[0]

data_path = working_dir + "/database/mimic3wdb/matched"
os.popen("mkdir " + working_dir + "/database/mimic3wdb/matched_numeric")
numeric_path = working_dir + "/database/mimic3wdb/matched_numeric"

level1 = os.popen("ls "+ data_path).read().split()

for l1 in level1:
	os.popen("mkdir " + numeric_path + "/" + l1)
	level2 = os.popen("ls " + data_path + "/" + l1).read().split()
	
	for l2 in level2:
		num_files = os.popen("ls " + data_path + "/" + l1 + "/" + l2 + "/*n.*").read().split()
			
		if len(num_files) != 0:
			os.popen("mkdir " + numeric_path + "/" + l1 + "/" + l2) # includes only folders that contain data, if wanna nclude all folders, place before if condition
			os.popen("cp " + data_path + "/" + l1 + "/" + l2 + "/*n.* " + numeric_path + "/" + l1 + "/" + l2)
