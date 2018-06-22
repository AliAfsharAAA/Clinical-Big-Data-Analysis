"""
This base template file will allow user to iterate over all patient records and just paste analysis code on a per
record basis below.  
"""
import os

#Get a list of the directories and paths of the data folders
database_dir = "/".join(os.popen("pwd").read().split()[0].split("/")[:-1])

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


#First iterate over ten folders
for d in dirs:

    folderPath = os.path.join(data_path,d)

    patientRecords = os.listdir(folderPath)

    #Now iterate over all patients
    for patient in patientRecords:

        patientPath = os.path.join(folderPath,patient)
        patientRecords = os.listdir(patientPath)


        #Get all numeric patient header files
        pRecords = [i for i in patientRecords if i.endswith('n.hea')]


        ###### If you are using WFDB package uncomment out the below line
        #recordName = recordName[:-4]
        ######

        ###### If you wish to only look at the first record, uncomment out first line
        #recordName = getFirstAdmittance(pRecords)
        ######

        #Iterate over all records
        for recordName in pRecords:
            filePath = os.path.join(patientPath,recordName)

            file = open(filePath,'r')

            lines = [line.rstrip('\n') for line in file]

            #Insert whatever code for analysis here




