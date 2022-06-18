# -*- coding: utf-8 -*-
#! Python3.8

# @by: Abraão Nascimento

import pandas   as pd
import psycopg2 as pg2 

from Config import Path, Database

class Output:

    '''
    Makes queries in PostgreSQL database and export the results to csv files
    '''

    def __init__(self): pass

    def conn(self):
        info = Database.info()
        conn = pg2.connect(
            dbname  = info['PG_DB'],
            user    = info['PG_USER'],
            host    = info['PG_HOST'],
            password= info['PG_PASSWORD'],
            port    = info['PG_PORT'],
        )
        return conn 

    def queries(self):  

        # Output 1: Total equipment failures that happened
        sql = '''
        SELECT COUNT(*) 
        FROM (SELECT DISTINCT equipment_id
            FROM logs
            LEFT JOIN equipment_sensors
            ON logs.sensor_id = equipment_sensors.sensor_id 
            WHERE logs.type = 'ERROR') AS query1
        '''

        # Output 2: Which equipment code had most failures
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

        # Output 3: Average amount of failures across equipment group, ordered by the number of failures in ascending order
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
        GROUP BY group_name, total
        ORDER BY total ASC
        '''

        queries = [{'Total_equipment_failures': sql}, 
                   {'Equipment_code_with_most_failures': sql2}, 
                   {'Average_amount_of_failures': sql3}]
        return queries

    def save(self): 
        '''
        Output is available insise the data/output folder
        '''
        queries = self.queries()
        for query in queries: 
            sql = list(query.values())[0]
            filename = list(query.keys())[0]
            output = pd.read_sql(sql, self.conn())
            output.to_csv(f'{Path.root_output()}{filename}')