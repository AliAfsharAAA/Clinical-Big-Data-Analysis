
from collections import defaultdict
import os
import csv

#Need to have the path of the master list of all patient records

def genSignalsDict():
    masterlist_path = './Signals_Dict.txt'
    masterlist = open(masterlist_path,'r')

    signalDict = {}

    for line in masterlist:
        vals = line.split('\t')
        header = vals[0]
        names = vals[1:]
        names = [i.rstrip() for i in names]
        names = list(filter(None, names))
        #print(header, ':',names)
        signalDict[header] = names
    return signalDict


def checkForSignal(signalName,signals,signalsDict):

    nameVars = signalsDict[signalName]

    for n in nameVars:
        if n in signals:
            return True
    return False


