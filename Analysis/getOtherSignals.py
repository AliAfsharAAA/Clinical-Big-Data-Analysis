import os
import wfdb
import numpy as np
import pickle
import ArtifactDetectors
signalDict = {
    'ABP Dias': ['ABPDias', 'ABP DIAS', 'ART DIAS', 'ABP Dias', 'ART Dias', 'AOBP Dias', 'Ao DIAS'],
    'ABP Mean': ['ABPMean', 'ABP MEAN', 'ART MEAN', 'ABP Mean', 'ART Mean', 'AOBP Mean', 'Ao MEAN'],
    'ABP Sys': ['ABPSys', 'ABP SYS','ABP Sys', 'ART SYS', 'ART Sys', 'AOBP Sys', 'Ao SYS'],
    'HR': ['HR'],
    'RESP': ['RESP'],
    'CVP': ['CVP'],
    'SpO2': ['SpO2','%SpO2','SpO2 l','SpO2 r','SpO2 L','SpO2 R'],
    'NBP Mean':['NBP MEAN','NBP Mean','NBPMean'],
    'NBP Dias':['NBP DIAS','NBP Dias','NBPDias'],
    'NBP Sys':['NBP SYS','NBP Sys','NBPSys']
}


otherSignals = ['HR','RESP','CVP','SpO2','NBP Mean','NBP Dias','NBP Sys']

folder = 'p08'
wfdbPath = '/Volumes/arc_drive'

minRecordsPath = 'records124'
secRecordsPath = 'records124DS'

minRecords = [i for i in  os.listdir(minRecordsPath)if i.startswith(folder)]
secRecords = [i for i in  os.listdir(secRecordsPath) if i.startswith(folder)]
import numpy as np


def detectCVPArtifact(signal):
    """
    :param signal: CVP signal (any frequency)
    artifacts
    <= 0
    > 30
    :return: List of artifact indices
    """
    indices = []

    for index, value in enumerate(signal):
        if not np.isnan(value):
            if value <= 0.0 or value > 30:
                indices.append(index)
    return indices


def detectRESPArtifact(signal):
    """
    :param signal: RESP signal (any frequency)
    artifacts:
    < 5
    > 50
    :return: List of artifact indices
    """
    indices = []
    for index, value in enumerate(signal):
        if not np.isnan(value):
            if value > 50 or value < 5:
                indices.append(index)
    return indices


def detectSpO2Artifact(signal):
    """
    :param signal: SpO2 signal (any frequency)
    artifacts:
    < 50
    :return: List of artifact indices
    """
    indices = []
    for index, value in enumerate(signal):
        if not np.isnan(value):
            if value < 50:
                indices.append(index)
    return indices


def detectNBPArtifact(sys, dias, mean):
    sysIndices = []
    diasIndices = []
    meanIndices = []

    for index, sysSignalValue in enumerate(sys):


        if sysSignalValue >= 300 or sysSignalValue <= 20:
            sysIndices.append(index)

            # # Condition 2A
            #
            # if sysSignalValue - dias[index] <= 5:
            #     sysIndices.append(index)
            #     diasIndices.append(index)
            #     meanIndices.append(index)
            #
            # # Condition 2B
            # if sysSignalValue - dias[index] > 200:
            #     # print('cond 2',sysSignalValue,dias[index])
            #     sysIndices.append(index)
            #     diasIndices.append(index)
            #     meanIndices.append(index)
            #
            # if sysSignalValue - mean[index] <= 3:
            #     sysIndices.append(index)
            #     diasIndices.append(index)
            #     meanIndices.append(index)



            if dias[index] <= 5 or dias[index] >= 225:
                # print('dias cond',diasSignalValue)
                diasIndices.append(index)

        # Condition 2A

        if round(sysSignalValue - dias[index], 2) <= 5:
            sysIndices.append(index)
            diasIndices.append(index)
            meanIndices.append(index)

        # Condition 2B
        if round(sysSignalValue - dias[index], 2) > 200:
            # print('cond 2',sysSignalValue,dias[index])
            sysIndices.append(index)
            diasIndices.append(index)
            meanIndices.append(index)

        if round(sysSignalValue - mean[index], 2) <= 3:
            sysIndices.append(index)
            diasIndices.append(index)
            meanIndices.append(index)

    return sysIndices,diasIndices,meanIndices


def detectHRArtifact(signal):
    """

    :param signal: HR signal
    artifacts:
    < 20
    > 220
    :return: List of artifact indices
    """
    indices = []

    for index, signalValue in enumerate(signal):

        if signalValue < 20 or signalValue > 220:
            indices.append(index)
    # print(indices)
    return indices

def verifynbp(sig):
    count = 0

    for value in sig:
        if count >14:
            return True
        if not np.isnan(value):
            count +=1

    if count > 14:
        return True

    return False

def verifySigLength(signal):

    cache = 0
    for index ,value in enumerate(signal):

        if isinstance(value,str):
            continue


        if not np.isnan(value) and value != 0:



            if cache > 14:
                return True
            cache += 1
            # print(cache)
        else:
            if cache > 14:
                return True
            cache = 0

    if cache > 14:
        return True

    return False


def artifact_to_nan(vector, indices):
    c = vector.copy()
    for value in indices:
        c[value] = np.nan

    return c

def getSignal(signal, record, signalsDict):
    variations = signalsDict[signal]
    for variation in variations:
        if variation in record['sig_name']:
            ind = record['sig_name'].index(variation)
            units = record['units'][ind]
            return record['p_signal'][:, ind]

    return None

def zero2Nan(signal):
    for index, value in enumerate(signal):
        if np.isnan(value) or value == 0.0:
            signal[index] = np.nan

    return signal

def nan2Zero(signal):
    for index, value in enumerate(signal):
        if np.isnan(value):
            signal[index] = 0.0

    return signal

def hasValidSignal(wfdbdict,signalName,fs):


    signal = getSignal(signalName,wfdbdict,signalDict)

    if signal is not None:
        rawSignal = zero2Nan(signal)


        #rawSignal = nan2Zero(signal)

        artifactD = 'detect' + signalName +'Artifact(signal)'

        AI = eval(artifactD)

        procsignal = artifact_to_nan(rawSignal,AI)
        #procsignal = nan2Zero(signal)

        if verifySigLength(procsignal):
            return True
        else:
            return False

    return False






def getName(pickleRecord):
    name = pickleRecord

    name = name[:-4]

    folder = name[:3]
    patient = name[:7]

    #print(folder,patient, name)


    return folder+'/'+patient+'/'+name

otherSignals = ['']
write= []

totalCount = 0

from collections import defaultdict

signalsCount = defaultdict(int)



for rec in secRecords:

    #print(getName(rec))

    wfdbName = getName(rec)
    totalCount +=1
    fullpath = os.path.join(wfdbPath,wfdbName)

    wfdbRec = wfdb.rdrecord(fullpath).__dict__

    signals = []

    for sig in ['HR','RESP','CVP','SpO2']:#NBP Mean','NBP Dias','NBP Sys']:

        if hasValidSignal(wfdbRec,sig,.0167):
            signals.append(sig)
            signalsCount[sig] += 1




    sys= getSignal('NBP Sys',wfdbRec,signalDict)
    dias = getSignal('NBP Dias', wfdbRec, signalDict)
    mean= getSignal('NBP Mean', wfdbRec, signalDict)

    if sys is not None and dias is not None and mean is not None:

        sys = zero2Nan(sys)
        dias = zero2Nan(dias)
        mean = zero2Nan(mean)

        if not np.isnan(sys).all():

            if verifynbp(sys):

                signals.append('NBP Sys')
                signalsCount['NBP Sys'] +=1
        if not np.isnan(dias).all():

            if verifynbp(dias):
                signals.append('NBP Dias')
                signalsCount['NBP Dias'] += 1

        if not np.isnan(mean).all():

            if verifynbp(mean):
                signals.append('NBP Mean')
                signalsCount['NBP Mean'] += 1



    l = [rec,signals]
    write.append(l)
    print(rec,signals)


print('for folder ' ,folder)
print(totalCount)
for key in signalsCount:
    print(key,signalsCount[key])

# import pandas as pd
# mydf = pd.DataFrame(write)
# mydf.to_csv('OtherSignalsDS',index = False,header = False)