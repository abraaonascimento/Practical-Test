# -*- coding: utf-8 -*-
#! Python3.8

# @by: Abraão Nascimento

import os
from environs import Env
from dataclasses import dataclass

env = Env()
env.read_env()

@dataclass
class Meta: 
    '''
    Store the meta info about the data as name and delimiters
    '''
    logs = 'equipment_failure_sensors.log'
    logs_delimiter = '\t'
    equipment = 'equipment.json'
    equipment_sensors = 'equipment_sensors.csv'
    equipment_sensors_delimiter = ';'

class Path:
    '''
    It provides information on standards paths, keys, main folders, etc. 
    '''

    @staticmethod
    def root():
        root = os.path.abspath(os.curdir)
        return root.replace('\\source_code','')

    @staticmethod
    def root_data():
        root_data  = f'{Path.root()}\\data\\'
        return root_data

    @staticmethod
    def root_output():
        root_output= f'{Path.root()}\\data\\output\\'
        return root_output

    @staticmethod
    def root_logs():
        root_output= f'{Path.root()}\\data\\logs\\'
        return root_output

class Database: 
    '''
    It provides connection to a PostgreSQL database
    '''

    @staticmethod
    def info():
        conn ={
            "PG_HOST"    :env('PG_HOST'),
            "PG_PORT"    :env('PG_PORT'),
            "PG_DB"      :env('PG_DB'),
            "PG_USER"    :env('PG_USER'),
            "PG_PASSWORD":env('PG_PASSWORD')
        }
        return conn