# -*- coding: utf-8 -*-
#! Python3.8

# @by: Abraão Nascimento

import re

def norm_logs(logs, date='2020-01'):
    """
    1 - Filter data by month and year and normalize it to be sent to a SQL database
    2 - date must be written as year-month
    """

    # Fields are not well structured, because of that they will be normalized and structured into 4 columns: datetime, type, message, sensor_id.
    # Merging and renaming fields
    logs.loc[:, 'message'] = logs[2] + ' ' + logs[3] + ' ' + logs[4] + ' ' +  logs[5]

    logs.rename(columns = {0:"datetime", 1:"type"}, inplace = True)
    logs = logs[["datetime","type","message"]]

    # Creating a new field for sensor_id
    messages   = logs.message.tolist()
    sensor_ids = [re.search('\[(\d+)\]', record).group(0) for record in messages] # Regex and list comprehension are used to find sensor IDs
    sensor_ids = [int(re.search('\d+',ID).group(0)) for ID in sensor_ids]

    logs['sensor_id'] = sensor_ids

    # Filtering Data
    logs_filter = logs[logs['datetime'].str.contains(date)==True]
    return logs_filter