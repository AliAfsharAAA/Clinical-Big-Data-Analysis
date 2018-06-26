import os
from collections import defaultdict


#Get a list of the directories and paths of the data folders
database_dir = "/".join(os.popen("pwd").read().split()[0].split("/")[:-1])
work_dir = os.popen("pwd").read().split()[0]
data_dir = "database/mimic3wdb/matched_num"
data_path = os.path.join(database_dir,data_dir)
dirs = [dr.split("/")[-2] for dr in os.popen("ls -d " + data_path + "/*/").read().split()]


def getFirstAdmittance(record_files):
    """
    This is a precautionary function in case some operations have changed order in patient record folders
    As a reminder file names are of format pXXNNNN-YYYY-MM-DD-hh-mm
    :param record_files: Patient record files, either numeric headers only or full directory
    :return: file name for the first admittance
    """
    files = [file for file in record_files if file.endswith('n.hea')]
    dates = []
    for index, file in enumerate(files):
        name, year, month, day, hour, minute = file.split('-')
        minute = minute[0:2]

        fullDate = int(year + month + day + hour + minute)

        dates.append(fullDate)

    return files[dates.index(min(dates))]

recordResults = []
counter = 0
counternpb = 0

def genSignalsDict():
    masterlist_path = os.path.join(database_dir,'metadata/Signals_Dict.txt')
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



signalsDict = genSignalsDict()
for s in signalsDict:
    print(s)
def checkForSignal(signalName, signals, signalsDict):
    nameVars = signalsDict[signalName]

    for n in nameVars:
        if n.strip() in signals:
            return True
    return False


for d in dirs:

    folderPath = os.path.join(data_path,d)

    patientRecords = os.listdir(folderPath)

    for patient in patientRecords:
        patientPath = os.path.join(folderPath,patient)

        patientRecords = os.listdir(patientPath)
        pRecords = [i for i in patientRecords if i.endswith('n.hea')]


        #recordName = getFirstAdmittance(patientRecords)

        #recordName = recordName[:-4]
        for recordName in pRecords:



            filePath = os.path.join(patientPath,recordName)
            #recordFile = wfdb.rdrecord(filePath)
            file = open(filePath,'r')

            signals = []


            for index, line in enumerate(file):
                if index == 0 or line.startswith('#'):
                    continue
                lines = line.split(' ')

                hopefullysignal = ' '.join(lines[8::])
                #print(hopefullysignal)
                signals.append(hopefullysignal.strip())
            #lines = [line.rstrip('\n') for line in file]
            #print(lines)
            print(signals)
            #print(recordName)


            #for k in signals:
                #print(k)
            if checkForSignal('NBP Sys', signals, signalsDict) and checkForSignal('ABP Sys', signals, signalsDict) and checkForSignal('Rhythm Status', signals, signalsDict) and checkForSignal('PAP', signals, signalsDict) and checkForSignal('CVP', signals, signalsDict):
                counter += 1
            else:
                pass


file = open('checksig.txt','w')

for r in recordResults:
    file.write(r)
file.write('\n')

file.write('Total num npb with abp: {}'.format(str(counter)))



file.close()