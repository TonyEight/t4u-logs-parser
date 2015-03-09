# -*- coding: utf-8 -*-
# Imports built-in modules
import os, sys, re, csv
from operator import itemgetter
from pprint import pprint
# Imports third-party modules based on requirements.txt
import simplejson
import dateutil.parser
from fabric.api import local


def parse_logs():
    """
    This functions retrives logs from the raw_logs directory and parses only
    telemetry files.
    It builds a CSV final log final that merges all traces.
    """
    ### directories definition
    FS_ENCODING = sys.getfilesystemencoding()
    THIS_FILE = __file__.decode(FS_ENCODING)
    BASE_DIR = os.path.dirname(os.path.realpath(os.path.abspath(THIS_FILE)))
    LOGS_DIR = os.path.join(BASE_DIR, 'raw_logs')

    ### uncompresses all log files
    # unzip the main file
    local('unzip %(dir)s/raw_logs.zip -d %(dir)s' % {'dir': LOGS_DIR})
    # retrieves all tarballs
    tars = [
        os.path.join(LOGS_DIR, f) for f in os.listdir(LOGS_DIR)\
        if os.path.isfile(os.path.join(LOGS_DIR, f)) and\
        os.path.join(LOGS_DIR, f)[-6:] == 'tar.gz'
    ]
    # extracts all logs from tarballs
    subdirs = []
    for tar in tars:
        # creates subdirectories for each machine
        subdir = tar.split('/')[-1].split('.')[0]
        subdir_path = os.path.join(LOGS_DIR, subdir)
        os.makedirs(subdir_path)
        subdirs.append(subdir_path)
        # extract to subdirectory
        local('tar -xvf %(tar)s -C %(path)s' % {
            'tar': tar,
            'path': subdir_path
        })

    ### merges all telemetry logs in one unique file
    # retrieves all telemetry logs
    telemetry_logs = []
    for directory in subdirs:
        for f in os.listdir(directory):
            if os.path.isfile(os.path.join(directory, f))\
            and os.path.join(directory, f)[-4:] == '.log'\
            and os.path.join(directory, f).split('/')[-1].find(
                'telemetry'
            ) != -1:
                telemetry_logs.append(os.path.join(directory, f))
    # creates unique log file
    merged_log = open(os.path.join(LOGS_DIR, 'merged_log.log'), 'wb')
    # merging operation
    content = [] 
    for log in telemetry_logs:
        with open(log, 'rb') as f:
            content += f.readlines()
    merged_log.writelines(content)
    merged_log.close()

    ### prepares CSV file
    csv_file = open(os.path.join(BASE_DIR, 'telemetry_logs.csv'), 'wb')
    # writer definition
    writer = csv.writer(csv_file, delimiter=';', quoting=csv.QUOTE_NONE)
    # csv header definition
    headers = [
        'Horodatage',
        'Application',
        'Version', 
        'IP',
        'ID Anonyme',
        'Type',
        'Trace',
        'Extra'
    ]
    # header writing
    writer.writerow(headers)
    # few metrics definition
    traces_count = 0
    users = []

    ### parses telemetry file
    # csv content buffer
    rows = []
    # log opening
    merged_log = open(os.path.join(LOGS_DIR, 'merged_log.log'), 'rb')
    # fetches each line from the log
    content = merged_log.readlines()
    # loop over lines
    for line in content:
        # as JSON is a strict format, we need to replace simple quotes
        # by double-quotes so simplejson can load the file without yelling
        # about errors around keys.
        json_line = line.replace('\'','"')
        # "json-load" the line
        data = simplejson.loads(json_line)
        # loop over traces in the line
        # one trace = one item in the DataLights list
        for trace in data['TelemetryDataLights']:
            # csv row buffer
            row = []
            timestamp = None
            # retrieve each info from the dictionnary created by the 
            # line loading starting with UTC time
            # here, we transform "UtcTime" field, which is a datetime string
            # in ISO format, to a python datetime object
            timestamp = dateutil.parser.parse(trace['UtcTime'])
            row.append(timestamp.strftime("%d-%m-%Y %H:%M:%S"))
            row.append(data['TelemetryDataAppInfo']['AppName'])
            row.append(data['TelemetryDataAppInfo']['AppVersion'])
            row.append(data['TelemetryDataAppInfo']['Ip'])
            row.append(data['TelemetryDataAppInfo']['LocalId'])
            row.append(trace['TelemetryDataType'])
            row.append(trace['Parameter'])
            # very minimal management of the "Extra" field
            if trace['Extra']:
                row.append(trace['Extra'].encode('utf-8'))
            else:
                row.append('')
            # different users count
            if data['TelemetryDataAppInfo']['LocalId'] not in users:
                users.append(data['TelemetryDataAppInfo']['LocalId'])
            traces_count += 1
            rows.append((timestamp, row))
    # log file closing
    merged_log.close()

    ### cleans LOGS_DIR
    for directory in subdirs:
        local('rm -fr %s' % directory)
    for tar in tars:
        local('rm -fr %s' % tar)

    ### finalizes CSV file
    # reorders each trace based on "UtcTime" field
    rows = sorted(rows, key=itemgetter(0))
    # writes the CSV content
    for row in rows:
        writer.writerow(row[1])
    # adds a separator line
    writer.writerow(['',''])
    # adds metrics to the CSV file
    writer.writerow(['Nombre de traces', traces_count])
    writer.writerow([r'Nombre de users différents', len(users)])
    # CSV file closing
    csv_file.close()


if __name__ == '__main__':
    """
    Calls the parse_logs function if the current file is directly called.
    """    
    parse_logs()
