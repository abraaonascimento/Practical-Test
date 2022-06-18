# -*- coding: utf-8 -*-
#! Python3.8

# @by: Abraão Nascimento

import psycopg2 as pg2 
import psycopg2.extras
from loguru import logger

from Config import Database

class Load:
    '''
    Load data into a PostgreSQL database
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

    def connect(self):
        connection= self.conn()
        psycopg2.extras.register_hstore(connection)
        return connection

    def execute(self, sql, params={}):
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)

    def logs(self, logs):
        sql='''
        INSERT INTO public.logs(datetime,
                              type,
                              message, 
                              sensor_id)

        SELECT unnest( %(datetime)s ),
               unnest( %(type)s),
               unnest( %(message)s),
               unnest( %(sensor_id)s)

        '''
        datetime  = logs.datetime.tolist()
        type      = logs.type.tolist()
        message   = logs.message.tolist()
        sensor_id = logs.sensor_id.tolist()
        self.execute(sql,locals())

    def equipment(self, code, equipment_id, group_name):
        sql='''
        INSERT INTO public.equipment(equipment_id,
                                     code,
                                     group_name)

        SELECT unnest( %(equipment_id)s ),
               unnest( %(code)s),
               unnest( %(group_name)s)
        '''
        self.execute(sql,locals())

    def equipment_sensors(self, equipment_sensors):
        sql='''
        INSERT INTO public.equipment_sensors(equipment_id,
                                             sensor_id)
        SELECT unnest( %(equipment_id)s ),
               unnest( %(sensor_id)s)
        '''
        sensor_id    = equipment_sensors.sensor_id.tolist()
        equipment_id = equipment_sensors.equipment_id.tolist()
        self.execute(sql,locals())

    def createLogs(self):
        """Create table to store the information of logs"""
        try:
            schema = "public"
            table  = 'logs'
            sql = "DROP TABLE IF EXISTS " + schema + f".{table};"
            sql = "CREATE TABLE IF NOT EXISTS " + schema + f".{table} "
            sql += "(sensor_id integer"
            sql += ",datetime character varying(100)"
            sql += ",type character varying(100)"
            sql += ",message character varying(1000)"
            sql += ")"
            self.execute(sql)
        except Exception as err:
            logger.exception(err)
            logger.exception('Creation of logs table failed')

    def createEquipment(self):
        """Create table to store the information of equipment"""
        try:
            schema = "public"
            table  = 'equipment'
            sql = "DROP TABLE IF EXISTS " + schema + f".{table};"
            sql = "CREATE TABLE IF NOT EXISTS " + schema + f".{table} "
            sql += "(equipment_id integer"
            sql += ",code character varying(100)"
            sql += ",group_name character varying(100)"
            sql += ")"
            self.execute(sql)
        except Exception as err:
            logger.exception(err)
            logger.exception('Creation of equipment table failed')

    def createEquipment_sensors(self):
        """Create table to store the information of equipment and sensors"""
        try:
            schema = "public"
            table  = 'equipment_sensors'
            sql =  "DROP TABLE IF EXISTS " + schema + f".{table};"
            sql += "CREATE TABLE " + schema + f".{table} "
            sql += "(equipment_id integer"
            sql += ",sensor_id integer"
            sql += ")"
            self.execute(sql)
        except Exception as err:
            logger.exception(err)
            logger.exception('Creation of equipment table failed')