# coding=utf-8
'''
Recon package to recoincilate data between two entities and return invalid data rows into DataFrame

'''
import json
import datetime
import time
from collections import namedtuple
from pyspark import HiveContext
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, IntegerType, StringType
from policies import validation as vd

# Define basic entities names
SRC_NAME = 'src'
DST_NAME = 'dst'

def get_table(table_name, df):
    '''
    Creates namedtuple

    Arguments:
        table_name (string):
        df (DataFrame): Spark data frame with data from 'table_name'

    Returns:
        namedtuple

    '''
    tbl = namedtuple('Table', 'name, data')
    return tbl(table_name, df)

def create_context():
    '''
    Creates spark context

    Returns:
        SparkSession

    '''
    conf = SparkConf()
    conf.set('spark.sql.shuffle.partitions', 100)
    conf.set('spark.sql.broadcastTimeout', 1200)
    conf.set('spark.shuffle.service.enabled', 'true')
    conf.set('spark.executor.cores', 2)
    conf.set('spark.executor.instances', 4)
    conf.set('spark.executor.memory', '2G')
    conf.set('spark.driver.cores', 2)
    conf.set('spark.driver.memory', '2G')

    spark = SparkSession.builder \
        .appName('Recon') \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()

    hivecontext = HiveContext(spark.sparkContext)
    hivecontext.setConf('hive.exec.dynamic.partition', 'true')
    hivecontext.setConf('hive.exec.dynamic.partition.mode', 'nonstrict')

    return spark

def get_context():
    ''' initialize context creation and return it    
    '''
    return create_context()

def get_config(file_path):
    '''
    Получение данных из файла json

    Arguments:
        file_path (String): путь до файла json

    Returns:
        Dict: json as dict data

    '''
    with open(file_path) as checks_file:
        data = json.load(checks_file)
    return data

def worker(table, key, checks):
    '''
    Функция запускает FlatMap для каждой строки входной таблицы
    На каждой строке прогоняются все проверки из словаря checks (json файл)

    '''

    timestamp = datetime.datetime.now()

    # returns True if ok, exception string if not ok
    def run_validation(src_data, dst_data, validation):
        '''
        Запуск валидации по имени метода

        '''

        validation = getattr(vd, validation['validationMethod'])(src_data, dst_data, **validation['properties'])
        return validation

    def do_map(row, key, checks):
        '''
        Процесс прохода всех валидаций в словаре проверок для строки
        В случае если в строке невалидными оказываются несколько колонок,
        Результатом будет несколько строк в каждой по одной ошибке на обработанную строку

        Returns:
            List(List(), List(), ..., List())

        '''
        invalid_data = []

        for column in checks:
            # Create column name: src_name, dst_name ...
            src_column = '{}_{}'.format(SRC_NAME, column['src_column_name'])
            dst_column = '{}_{}'.format(DST_NAME, column['dst_column_name'])

            validation = column['validation']

            # if data is conciliate then returns None
            bad_data_desc = run_validation(src_data=row[src_column], dst_data=row[dst_column], validation=validation)

            if not bad_data_desc:
                continue

            # Создание строки с данным и ошибкой
            #
            # Добавление ключа
            invalid_data.append([row[key]])

            # Добавление остальных данных
            invalid_data[-1].extend(
                [
                    src_column,                     # src column name
                    dst_column,                     # dst column name
                    validation['validationMethod'], # validation method name
                    str(row[src_column]),           # src column value
                    str(row[dst_column]),           # dst column value
                    bad_data_desc,                  # String exception description
                    str(timestamp),                 # current datetime
                    timestamp.strftime('%Y-%m-%d')  # current date
                ]
            )

        return invalid_data

    # Обработка
    return table.rdd.flatMap(
        lambda row: do_map(
            row=row,
            key=key,
            checks=checks
        )
    )

def rename_columns(prefix, data, exclude=[]):
    '''
    Добавление к названиям колонок заданного префикса для исключения дублирования одинаковых имен
    column_name >> prefix_column_name

    Arguments:
        prefix (string): Добавляемый префикс
        data (DataFrame): Датафрейм для обработки
        exclude (string/List): Колонки которые не нужно переименовывать

    Returns:
        DataFrame

    '''
    for column in [column for column in data.columns if column not in exclude]:
        data = data.withColumnRenamed(
            column,
            '{}_{}'.format(prefix, column)
        )

    return data

class Recon(object):
    def __init__(self, *args, **kwargs):
        self.validations = get_config(kwargs.get('j'))
        self.spark = get_context()
        self.src_table = get_table(
            kwargs.get(SRC_NAME),
            self.get_df(kwargs.get(SRC_NAME))
        )
        self.dst_table = get_table(
            kwargs.get(DST_NAME),
            self.get_df(kwargs.get(DST_NAME))
        )

    def get_df(self, table_name):
        '''
        Arguments:
            table_name (string)

        Returns:
            spark DataFrame

        '''
        return self.spark.table(table_name)
    
    def get_columns_to_valid(self):
        ''' Returns only columns dictionaries from json config file

        Returns:
            >>> [
            >>>     {
            >>>         "dst_column_name": "day",
            >>>         "src_column_name": "day",
            >>>         "validation": {
            >>>             "properties": {},
            >>>             "validationMethod": "date_validator"
            >>>         }
            >>>     },
            >>>     {
            >>>         ...
            >>>     },
            >>>     ...
            >>> ]
        '''
        return [val for key, val in self.validations.iteritems() if key.startswith('col_')]

    def join_df(self):
        ''' Performs full join operation on both given DataFrames
        '''
        src = rename_columns(SRC_NAME, self.src_table.data, exclude=self.validations.get('key'))
        dst = rename_columns(DST_NAME, self.dst_table.data, exclude=self.validations.get('key'))

        data = src.join(
            dst,
            [self.validations.get('key')],
            'full'
        )

        # stdout Full join result
        data.show()

        return data

    def create_dataframe(self, data):
        ''' Create DataFrame entity from list of lists (can be done dynamically from json representation of Structure)
        '''
        data = self.spark.createDataFrame(data, StructType([
            StructField('data_key_value', StringType()),
            StructField('src_column_name', StringType()),
            StructField('dst_column_name', StringType()),
            StructField('validation_method', StringType()),
            StructField('src_column_value', StringType()),
            StructField('dst_column_value', StringType()),
            StructField('description', StringType()),
            StructField('check_dttm', StringType()),
            StructField('check_date', StringType())
        ]))

        return data
    
    def run(self):
        ''' Runs all job stages in order
        '''
        columns_to_valid = self.get_columns_to_valid()
        data = self.join_df()
        invalid_data = worker(table=data, key=self.validations.get('key'), checks=columns_to_valid)
        data = self.create_dataframe(invalid_data)

        # Show first 100 rows of result
        data.show(100, False)
