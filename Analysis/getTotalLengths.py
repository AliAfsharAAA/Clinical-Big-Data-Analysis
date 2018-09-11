import pickle
import os
from collections import defaultdict
import numpy as np

import json
from datetime import datetime


picklePath = '/Volumes/arc_drive/processedRecords'

objects = list(set([file for file in os.listdir(picklePath) if file.endswith('.pkl') and file.startswith('p02')]))
objects.sort()
print(len(objects))

minuteData = 0
secondData = 0
mincount =0
seccount = 0
for obj in objects:
    #print(obj)
    path = os.path.join(picklePath,obj)
    with open(path, 'rb') as input:
        recordObj = pickle.load(input)

        input.close()

        sig_len = recordObj['sig_len']

        fs = recordObj['fs']

        signals = recordObj['signals']

        if signals.get('ABPsys') is not None:
            if fs == 1.0:
                secondData += int(sig_len)
                seccount +=1

            else:
                minuteData += int(sig_len)
                mincount+=1
        #print(mincount,seccount,mincount+seccount)



print('Minute length ',minuteData)
print('Second length ',secondData)
