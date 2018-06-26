


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

p_and_locations = defaultdict(list)

for d in dirs:

    folderPath = os.path.join(data_path,d)

    patientRecords = os.listdir(folderPath)

    for patient in patientRecords:
        patientPath = os.path.join(folderPath,patient)

        patientRecords = os.listdir(patientPath)
        pRecords = [i for i in patientRecords if i.endswith('n.hea')]


        #recordName = getFirstAdmittance(patientRecords)


        locations = []
        #recordName = recordName[:-4]
        for recordName in pRecords:
            filePath = os.path.join(patientPath,recordName)
            #recordFile = wfdb.rdrecord(filePath)
            file = open(filePath,'r')

            lines = [line.rstrip('\n') for line in file]
            #print(lines)
            for line in lines:
                if line.strip().startswith('# Location'):
                    loc, location = line.split(':')
                    location = location.strip()

                    # record = recordFile.__dict__
                    # location = getLocation(record)
                    # ABP = hasABP(record)
                    # print(patient,location,ABP)
                    p_and_locations[patient] += location





output = open('patientAndLocations.csv','w')

for patient in p_and_locations:
    output.write(patient,' ',','.join(p_and_locations[patient]))

output.close()


# changed = 0
# unchanged = 0
#
# for ll in listlocations:
#     if set(ll) == ll:
#         unchanged += 1
#         changed +=1
#
#
# print('moved {}'.format(changed))
# print('unmoved {}'.format(unchanged))
# print('% = {}'.format(changed/(unchanged+changed)))

    # else:
# with open('allLocations.csv', 'w') as f:
#     f.write('Signal | Count')
#     f.write('\n')
#     for key in locations:
#         line = key.rstrip() + ': ' + str(locations[key])+'\n'
#         f.write(line)
#     #[f.write('{0}: {1} \n'.format(key, value)) for key, value in signals.items()]
#
#     f.write('Total: {} \n'.format(listed))
#     f.write('Tota Count : {} \n'.format(emptycount))
#
#
# f.close()