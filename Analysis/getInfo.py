
#import wfdb
import csv
import os
from collections import defaultdict
#import argparse

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

locations = defaultdict(int)

emptycount = 0
for d in dirs:

    folderPath = os.path.join(data_path,d)
    print(folderPath)
    patientRecords = os.listdir(folderPath)

    for patient in patientRecords:
        patientPath = os.path.join(folderPath,patient)

        patientRecords = os.listdir(patientPath)
        emptycount +=1
        recordName = getFirstAdmittance(patientRecords)

        #recordName = recordName[:-4]
        filePath = os.path.join(patientPath,recordName)
        #recordFile = wfdb.rdrecord(filePath)
        with open(filePath) as file:
            lines = [line.rstrip('\n') for line in file]

        for line in lines:
            if line.strip().startswith('# Location'):
                loc, location = line.split(':')
                location = location.strip()

                locations[location] += 1



listed = 0
for k in locations:
    listed += locations[k]
with open('locations.csv', 'w') as f:
    f.write('Unit | Count')
    f.write('\n')
    [f.write('{0}: {1}\n'.format(key, value)) for key, value in locations.items()]

    f.write('Total: {} \n'.format(listed))
    f.write('Missing: {}'.format(emptycount-listed))

f.close()

#
#
# parser = argparse.ArgumentParser()
# parser.add_argument('folder',type = str,help = "folder like p0# to analyze")
# arg = parser.parse_args()
# folder = arg.folder
#
# directory = '/home-1/acrank2@jhu.edu/work/database/mimic3wdb/matched_num/'

#folderName = os.path.join(directory,folder)
folderName = './mimic3wdb/matched_numeric/p03'
folders = os.listdir(folderName)

folders.sort()


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


def hasABP(recordDict):
    """
    Determine if record has any form of ABP recording
    :param recordDict: wfdb record dictionary (record.__dict__)
    :return: Boolean if an ABP signal is present in the monitored signals
    """
    signals = [i.lower().split() for i in recordDict['sig_name']]
    signals_flat_list = [item for sublist in signals for item in sublist]
    return ('abp' in signals_flat_list)






def getLocation(recordDict):
    """
    Retrieve specific ICU location for a patient record
    :param recordDict: wfdb record dictionary (record.__dict__)
    :return: ICU location for a patient record, else None
    """
    comments = recordDict['comments']
    for comment in comments:
        if comment.lower().startswith('location:'):
            head, location = comment.split(':')
            location = location.strip()
            return location

    return None


count = 0
locations =defaultdict(int)
for patient in folders:



    patient_path = os.path.join(folderName,patient)
    if not os.listdir(patient_path):
        continue


    indivRecords = os.listdir(patient_path)
    count += 1
    recordName = getFirstAdmittance(indivRecords)
    recordName = recordName[:-4]
    filePath = os.path.join(patient_path,recordName)
    recordFile = wfdb.rdrecord(filePath)

    record = recordFile.__dict__
    location = getLocation(record)
    ABP = hasABP(record)
    #print(patient,location,ABP)
    locations[location] += 1



print('\n')
print(count)
for k in locations:
    print(k,locations[k])




