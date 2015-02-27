# -*- coding: utf-8 -*-
# Imports built-in modules
import os, sys, re, csv
from operator import itemgetter
from pprint import pprint
# Imports third-party modules based on requirements.txt
import simplejson
import dateutil.parser


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
    
    ### gets all log files
    files = [
        os.path.join(LOGS_DIR, f) for f in os.listdir(LOGS_DIR)\
        if os.path.isfile(os.path.join(LOGS_DIR, f))
    ]
    
    ### retrieves thoses telemetry ones
    telemetry_files = []
    for log_file in files:
        # all files containing 'telemetry' as 9 last characters
        if log_file[-9:] == 'telemetry':
            telemetry_files.append(log_file)

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

    ### parses telemetry files
    # csv content buffer
    rows = []
    # loop over telemetry logs
    for log in telemetry_files:
        # log opening
        current_log = open(log, 'rb')
        # fetches each line from the log
        content = current_log.readlines()
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
                # retrieve each info from the dictionnary created by the 
                # line loading starting with UTC time
                # here, we transform "UtcTime" field, which is a datetime string
                # in ISO format, to a python datetime object
                row.append(
                    dateutil.parser.parse(trace['UtcTime']
                ).strftime("%d-%m-%Y %H:%M:%S"))
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
                rows.append(row)
        # log file closing
        current_log.close()

    ### finalizes CSV file
    # reorders each trace based on "UtcTime" field
    rows = sorted(rows, key=itemgetter(0))
    # writes the CSV content
    writer.writerows(rows)
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
