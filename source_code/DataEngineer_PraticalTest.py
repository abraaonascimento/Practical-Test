# -*- coding: utf-8 -*-
#! Python3.8

# @by: Abraão Nascimento

import re
import os
import pandas as pd
from loguru import logger
from sqlalchemy import create_engine

# ## Summary:
# 
# __1.__ Data exploration <br>
# __2.__ In Memory Database <br>
# __3.__ Output 1: Total equipment failures that happened<br>
# __4.__ Output 2: Which equipment code had most failures <br>
# __5.__ Output 3: Average amount of failures across equipment group, ordered by the number of failures in ascending order<br> 
# __6.__ Contact <br> 

def data_get(): 

      equipment_sensors = pd.read_csv(f'{root_data}equipment_sensors.csv', sep=';') # If you are using Linux, replace \\ to /, please.

      # #### 1.2 Data equipment.json
      equipment = pd.read_json(f'{root_data}equipment.json')

      # It was possible to check that columns are integer and string format, and there are no null values and no duplicates in the equipment_id field.
      # #### 1.2 Data equipment_failure_sensors.log

      logs = pd.read_csv(f'{root_data}equipment_failure_sensors.log', sep='\t', header=None)
      return logs,equipment,equipment_sensors

def data_norm(logs, date='2020-01'):
      """
      date must be written as year-month
      """

      # Fields are not well structured, because of that they will be normalized and structured into 4 columns: datetime, type, message, sensor_id.
      # 1.2.1  Merging and renaming fields
      logs.loc[:, 'message'] = logs[2] + ' ' + logs[3] + ' ' + logs[4] + ' ' +  logs[5]

      logs.rename(columns = {0:"datetime", 1:"type"}, inplace = True)
      logs = logs[["datetime","type","message"]]

      # 1.2.2 Creating a new field for sensor_id
      messages   = logs.message.tolist()
      sensor_ids = [re.search('\[(\d+)\]', record).group(0) for record in messages] # Regex and list comprehension are used to find sensor IDs
      sensor_ids = [re.search('\d+',ID).group(0) for ID in sensor_ids]

      logs['sensor_id'] = sensor_ids

      # 1.2.3 Filtering Data
      logs_01_2020 = logs[logs['datetime'].str.contains(date)==True]
      return logs

def data_load(logs,equipment,equipment_sensors): 
      # 2 - In Memory Database
      # To query the data via SQL a In Memory Database has been created. As these files are not complex files of 4-8 gigabytes size should be executed with big issues. However, there are other possibilities to handle gigabytes-size data. One of them is to send the data in batches to a cloud or local database and execute the queries there.

      # 2.1 Creating a in memory database
      in_memory_db = create_engine('sqlite://', echo=False)

      # 2.2 Loading the data to the database created
      logs.to_sql('logs', con=in_memory_db)
      equipment.to_sql('equipment', con=in_memory_db)
      equipment_sensors.to_sql('equipment_sensors', con=in_memory_db)
      return in_memory_db

def data_save(in_memory_db): 
      """
      It is using data loaded in memory, but this can also be connected to local or cloud databases
      """

      # 3 - Output 1: Total equipment failures that happened
      sql = '''
      SELECT COUNT(*) 
      FROM (SELECT DISTINCT equipment_id
            FROM logs
            LEFT JOIN equipment_sensors
            ON logs.sensor_id = equipment_sensors.sensor_id 
            WHERE logs.type = 'ERROR')
      '''

      output = {'Total equipment failures': [in_memory_db.execute(sql).fetchall()[0][0]]}
      output = pd.DataFrame(data=output)
      output.to_csv(f'{root_output}Total_equipment_failures.csv', encoding='utf8', index=False)

      # 4 - Output 2: Which equipment code had most failures
      sql2 = '''
      SELECT code, COUNT(code) AS total FROM (SELECT equipment_id
                  FROM logs
                  LEFT JOIN equipment_sensors
                  ON logs.sensor_id = equipment_sensors.sensor_id 
                  WHERE logs.type = 'ERROR') AS query1
      LEFT JOIN equipment
      ON query1.equipment_id = equipment.equipment_id
      GROUP BY code
      ORDER BY total DESC
      '''
      data = in_memory_db.execute(sql2).fetchall()

      output2 = {'Equipment code with most failures': [data[0][0]], 'Total of failures': [data[0][1]]}
      output2 = pd.DataFrame(data=output2)
      output2.to_csv(f'{root_output}Equipment_code_with_most_failures.csv', encoding='utf8', index=False)

      # 5 - Output 3: Average amount of failures across equipment group, ordered by the number of failures in ascending order
      sql3 = '''
      SELECT group_name, CAST(ROUND(AVG(total)) AS INTEGER)
      FROM  (SELECT code, COUNT(code) AS total 
            FROM (SELECT equipment_id
            FROM logs
            LEFT JOIN equipment_sensors
            ON logs.sensor_id = equipment_sensors.sensor_id 
            WHERE logs.type = 'ERROR') AS query1
      LEFT JOIN equipment
      ON query1.equipment_id = equipment.equipment_id
      GROUP BY code) AS query2
      LEFT JOIN equipment
      ON query2.code = equipment.code
      GROUP BY group_name
      ORDER BY total ASC
      '''
      data = in_memory_db.execute(sql3).fetchall()

      output3 = {'Group_name': [line[0] for line in data ], 'Average': [line[1] for line in data]}
      output3 = pd.DataFrame(data=output3)
      output3.to_csv(f'{root_output}Average_amount_of_failures.csv', encoding='utf8', index=False) # Average has been formatted to integer, but it also can be decimal if needed

if __name__ == "__main__":

      logger.info('Process has been started.')

      root = os.path.abspath(os.curdir).replace('\\source_code','') # The main directory of project is kept in the variable root. This may have a slice difference in Linux.
      root_data  = f'{root}\\data\\'
      root_output= f'{root}\\data\\output\\'

      logs,equipment,equipment_sensors = data_get()
      logs = data_norm(logs)
      in_memory_db = data_load(logs,equipment,equipment_sensors)
      data_save(in_memory_db)

      logger.info('Process has been completed!')