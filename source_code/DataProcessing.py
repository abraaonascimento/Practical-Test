# -*- coding: utf-8 -*-
#! Python3.8

# @by: Abraão Nascimento

import ijson
import numpy    as np
import pandas   as pd

from datetime import datetime
from DataLoad import Load
from DataNorm import norm_logs
from DataSaveQueries import Output
from Config   import Path, Meta
from loguru import logger

@logger.catch
def processing(chunksize=100_000): 
    dataload = Load()
    dataload.connect()
    dataload.createLogs()
    dataload.createEquipment()
    dataload.createEquipment_sensors()

    for chunk in pd.read_csv(f'{Path.root_data()}{Meta.logs}', sep=Meta.logs_delimiter, chunksize=chunksize, header=None): 
        data = chunk.replace(np.nan, '', regex=True)
        data = norm_logs(data)
        dataload.logs(data)

    for chunk in pd.read_csv(f'{Path.root_data()}{Meta.equipment_sensors}', sep=Meta.equipment_sensors_delimiter, chunksize=chunksize):
        data = chunk.replace(np.nan, '', regex=True)
        dataload.equipment_sensors(data)

    chunk = 0
    code, equipment_id, group_name = [],[],[]
    # Reading JSON file on-demand. Chuck size: 100_000
    with open(f'{Path.root_data()}{Meta.equipment}', "rb") as f:
        for record in ijson.items(f, "item"):
            chunk +=1
            code.append(record['code'])
            equipment_id.append(record['equipment_id'])
            group_name.append(record['group_name'])
            if chunk >= 100_000:
                chunk = 0
                dataload.equipment(code, equipment_id, group_name)
                # load data into the database and release used memory
                del code, equipment_id, group_name
                code, equipment_id, group_name = [],[],[]
        if chunk < 100_000: 
            dataload.equipment(code, equipment_id, group_name)
            del code, equipment_id, group_name

if __name__ == "__main__":
    
    date = datetime.now()

    # Save log file inside data/logs folder
    logger.add(f'{Path.root_logs()}Date{date.strftime("%Y%m%d")}_Time{date.strftime("%H%M%S")}.log.log', diagnose = True, enqueue=True)

    logger.info('Process has been started.')
    processing()
    logger.info('Data Processing has been completed!')
      
    logger.info('Creating output files')

    output = Output()
    output.save()

    logger.info('Data save has been saved and is available into the data/output folder.')

      