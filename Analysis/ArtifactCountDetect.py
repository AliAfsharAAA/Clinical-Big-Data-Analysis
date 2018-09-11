import pickle
import os
from collections import defaultdict
import numpy as np

import json
from datetime import datetime


#picklePath = '/Macintosh HD/Users/arc/Documents/records122'
picklePath ='records124AVGDS'

folder = ''


if folder != '':
    objects = [file for file in os.listdir(picklePath) if file.endswith('.pkl') and file.startswith(folder)]
else:
    objects = [file for file in os.listdir(picklePath) if file.endswith('.pkl')]# and file.startswith('folder')]

objects = list(set(objects))
print('Total records ',len(objects))
totalRecords = len(objects)
objects.sort()


#objects = [i for i in objects if i.startswith('p02')]



#'p023594-2105-02-10-10-43n.pkl' p086379-2155-09-17-20-26n.pkl']

signalType= 'processed'
FREQUENCY =.0167




Condition1 = 0

Condition2A = 0
Condition2B = 0
Condition2C = 0

Condition3 = 0

Condition4sys = 0
Condition4dias = 0
Condition4mean = 0


Condition5sys = 0
Condition5dias = 0
Condition5mean = 0


def CountDetectABPArtifact(sys, dias, mean, fs,verbose = False):
    """


    Using a similar approach then Sun et al.

    Condition 1: SBP >= 300 or SBP <= 20
    Condition 2a: SBP - DBP <= 5
               b  SBP - DBP > 200

    Condition 3: DBP <=5 or DBP >= 225

    Condition 4: BP beat-to beat > 20mmhg for Ps,Pd, mean

    Condition 5: += 80 mmhg / min change

    """


    cond1 = 0

    cond2a = 0
    cond2b = 0
    cond2c = 0


    cond3 = 0

    cond4sys = 0
    cond4dias = 0
    cond4mean = 0

    cond5sys = 0
    cond5dias = 0
    cond5mean = 0

    for index, sysSignalValue in enumerate(sys):

        # Condition 1
        if not np.isnan(sysSignalValue):
            if sysSignalValue >= 300 or sysSignalValue <= 20:

                cond1 +=1

        # Condition 2
        # < mean < sys:
        if not np.isnan(sysSignalValue) and not np.isnan(dias[index]):
            if round(sysSignalValue - dias[index],2) <= 5:
                cond2a +=1
            if sysSignalValue - dias[index] > 200:
                # print('cond 2',sysSignalValue,dias[index])

                cond2b +=1

        if round(sysSignalValue - mean[index],2) <=3:
            cond2c +=1

    # Condition 3
    for index, diasSignalValue in enumerate(dias):

        if not np.isnan(diasSignalValue):
            if diasSignalValue <= 5 or diasSignalValue >= 225:


                cond3 += 1

    def detectDelta(signal, delta):
        flaggedsig = []
        for index, sigVal in enumerate(signal[:-1]):
            if not np.isnan(sigVal) and not np.isnan(signal[index + 1]):
                if abs(signal[index + 1] - sigVal) >= delta:
                    # flaggedsig.append(index)
                    flaggedsig.append(index + 1)
        return flaggedsig

    # def ConditionFive(signal,delta):
    #
    #     # Can't operate if signal is less than a minute
    #     if len(signal) <= 60:
    #         return 0
    #
    #     badminutes = 0
    #     for index, value in enumerate(signal[:-60]):
    #
    #         # print(index,value,'       ',index+60,signal[index+60])
    #         vector = signal[index:index + 60]
    #
    #         if abs(np.amax(vector) - np.amin(vector)) >= delta:
    #             # print(index,index+60)
    #             # print([i for i in range(index,index+60)])
    #             #print('ROLLING WINDOW',record)
    #             badminutes += 1
    #             #indices.append(i for i in range(index, index + 60))
    #
    #     #if indices != []:
    #         #ndices = [item for sublist in indices for item in sublist]
    #
    #     return badminutes #list(set(indices))




    #CONDITION 5
    for index, signal in enumerate(sys[:-1]):
        # Sys

        if not np.isnan(signal) and not np.isnan(sys[index + 1]):
            if abs(sys[index + 1] - signal) >= 80:
                cond5sys += 1
        # Dias
        if not np.isnan(dias[index]) and not np.isnan(dias[index + 1]):
            if round(abs(dias[index + 1] - dias[index]),2) >= 40:
                cond5dias += 1
                quit()
        # Mean
        if not np.isnan(mean[index]) and not np.isnan(mean[index + 1]):
            if round(abs(mean[index + 1] - mean[index]),2) >= 40:
                cond5mean += 1



    return cond1,cond2a,cond2b,cond2c,cond3,cond4sys,cond4dias,cond4mean,cond5sys,cond5dias,cond5mean


print('Preparing to run for {} Hz and {} data'.format(FREQUENCY,signalType))


minutes = 0
seconds = 0
mincount = 0
seccount = 0

lines = []

count = 0
#bjects = ['/Users/arc/PycharmProjects/work/records121/p027374-2111-06-06-18-08n.pkl']
for record in objects:
        print('\n')
        #print('On record {} - {} / {} -  {} %'.format(record,index,totalRecords,(index/totalRecords)*100))



        picklename = os.path.join(picklePath,record)
        with open(picklename, 'rb') as input:

            recordObj = pickle.load(input)

            input.close()

            signals = recordObj['signals']
            fs = .0167




            count +=1
            rawSys = signals['ABPsys'][signalType]
            rawDias = signals['ABPdias'][signalType]
            rawMean = signals['ABPmean'][signalType]



            c1,c2a,c2b,c2c,c3,c4sys,c4dias,c4mean,c5sys,c5dias,c5mean = CountDetectABPArtifact(rawSys, rawDias,rawMean, fs)


            Condition1 += c1

            Condition2A += c2a
            Condition2B += c2b
            Condition2C += c2c

            Condition3 += c3

            Condition4sys += c4sys
            Condition4dias += c4dias
            Condition4mean += c4mean

            Condition5sys += c5sys
            Condition5dias+= c5dias
            Condition5mean += c5mean

            l = [str(record[:-4]),c1,c2a,c2b,c2c,c3,c5sys,c5dias,c5mean]
            print(l)
            lines.append(l)



if folder == '':
    title = 'ALL' +picklePath +signalType + str(FREQUENCY) + '.csv'
else:
    title = folder +picklePath +signalType + str(FREQUENCY) + '.csv'

import pandas as pd
mydf = pd.DataFrame(lines)
mydf.to_csv(title,index = False,header = False)


# fileWriteTime = "{:%B %d, %Y}".format(datetime.now())
#
print(picklePath)
print('For {} Frequency and {} - # {} '.format(FREQUENCY,signalType,count))
print('Cond 1: ',Condition1)

print('Cond 2A: ',Condition2A)
print('Cond 2B: ',Condition2B)
print('Cond 2C: ',Condition2C)

print('Cond 3: ',Condition3)

print('Cond 4 sys: ',Condition4sys)
print('Cond 4 dias: ',Condition4dias)
print('Cond 4 mean: ',Condition4mean)

print('Cond 5 sys: ',Condition5sys)
print('Cond 5 dias: ',Condition5dias)
print('Cond 5 mean: ',Condition5mean)


# filename = 'ArtifactCount_{}_Hz_{}_data_count_#{}_{}_partition {}.txt'.format(FREQUENCY,signalType,count,fileWriteTime,partition)
#
# with open(filename, 'w') as file:
#     file.write('Cond 1: {} \n'.format(Condition1))
#     file.write('Cond 2 {} \n'.format(Condition2))
#     file.write('Cond 3: {} \n'.format(Condition3))
#
#     file.write('Cond 4sys: {} \n'.format(Condition4sys))
#     file.write('Cond 4dias: {} \n'.format(Condition4dias))
#     file.write('Cond 4mean: {} \n'.format(Condition4mean))
#
#     file.write('Cond 5sys: {} \n'.format(Condition5sys))
#     file.write('Cond 5dias: {} \n'.format(Condition5dias))
#     file.write('Cond 5mean: {} \n'.format(Condition5mean))
#
# file.close()