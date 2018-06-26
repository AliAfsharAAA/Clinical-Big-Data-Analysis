


import os
from collections import defaultdict


#Get a list of the directories and paths of the data folders
database_dir = "/".join(os.popen("pwd").read().split()[0].split("/")[:-1])
work_dir = os.popen("pwd").read().split()[0]
data_dir = "database/mimic3wdb/matched_num"
data_path = database_dir + "/" + data_dir
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

signals = defaultdict(int)

emptycount = 0
for d in dirs:

    folderPath = os.path.join(data_path,d)

    patientRecords = os.listdir(folderPath)

    for patient in patientRecords:
        patientPath = os.path.join(folderPath,patient)

        patientRecords = os.listdir(patientPath)
        pRecords = [i for i in patientRecords if i.endswith('n.hea')]
        #emptycount +=1

        #recordName = getFirstAdmittance(patientRecords)

        #recordName = recordName[:-4]
        for recordName in pRecords:
            filePath = os.path.join(patientPath,recordName)
            #recordFile = wfdb.rdrecord(filePath)
            file = open(filePath,'r')




            for index, line in enumerate(file):
                if index == 0:
                    continue
                lines = line.split(' ')
                hopefullysignal = ' '.join(lines[8::])

                signals[hopefullysignal] += 1

listed = 0
for k in signals:
    listed += signals[k]
with open('signals.csv', 'w') as f:
    f.write('Signal | Count')
    f.write('\n')
    for key in signals:
        line = key.rstrip() + ': ' + str(signals[key])+'\n'
        f.write(line)
    #[f.write('{0}: {1} \n'.format(key, value)) for key, value in signals.items()]

    f.write('Total: {} \n'.format(listed))


f.close()