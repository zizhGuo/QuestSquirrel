import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

import contextlib
from decorator import retry
from clickhouse_driver import Client as CHClient
import pymysql
import pandas as pd

from abc import abstractmethod

# @staticmethod
# @retry(max_retries=3, retry_delay=5, type='MySQL Connector', exception_to_check=Exception)
# @contextlib.contextmanager
# def mysql_client(host, user, password, database):
#     connection = None
#     try:
#         if database != 'na':
#             connection = pymysql.connect(
#                 host=host,
#                 user=user,
#                 password=password,
#                 database=database
#             )
#         else:
#             print(f'database: {database}')
#             connection = pymysql.connect(
#                     host=host,
#                     user=user,
#                     password=password   
#             )
#         yield connection
#     except Exception as e:
#         print('Failed to connect to MySQL, error:', e)
#         if connection is not None:
#             connection.close()
#     finally:
#         if connection is not None and connection.open:
#             connection.close()

# @staticmethod
# @retry(max_retries=3, retry_delay=5, type='contextmanager: ClickHouse connector', exception_to_check=Exception)
# @contextlib.contextmanager
# def clickhouse_client(host, port, user, password):
#     client = None
#     try:
#         client = CHClient(host=host, port=port, user=user, password=password)
#         yield client
#     except Exception as e:
#         print('Failed to connect to ClickHouse, error:', e)
#         if client is not None:
#             client.disconnect()
#     finally:
#         if client is not None:
#             client.disconnect()


# @contextlib.contextmanager
# def clickhouse_client(client: CHClient):
#     try:
#         yield client
#     except Exception as e:
#         print('connect clickhouse failed, error: ', e)
#     finally:
#         client.disconnect()

class Client:
    def __init__(self) -> None:
        pass
    
    # parent
    @staticmethod
    def _create_sparkcontext(name):
        try:
            spark = SparkSession.builder.appName(name).getOrCreate()
            spark.conf.set("spark.sql.session.timeZone", "UTC")
        except Exception as e:
            print(f'create sparksession and sparkcontext failed: {e}')
            return None
        else:
            return spark

    # parent
    @property
    def client(self):
        if not hasattr(self, '_client'):
            self._client = self._create_connector(self.host, self.port, self.user, self.password)
        return self._client
    
    # parent
    @property
    def spark(self):
        if not hasattr(self, '_spark'):
            self._spark = self._create_sparkcontext()
        return self._spark

    # parent
    @property
    def result(self, **kwargs):
        if not hasattr(self, '_result'):
            if 'sql' in kwargs:
                sql = kwargs['sql']
            else:
                sql = self.sql
            self._result = self.execute(sql)
        return self._result
    
    # parent
    @staticmethod
    def data2pandasdf(data, columns):
        return pd.DataFrame(data, columns=columns)
    
    # parent
    @staticmethod
    def create_spark_df(spark, df:pd.DataFrame, schema:StructType):
        return spark.createDataFrame(df, schema=schema)

    # parent
    @staticmethod
    def write_files(df, partitionkey, save_path):
        if_path_existed =  os.path.exists(save_path)
        if not if_path_existed and partitionkey not in df.columns:
            # 不存在表，但是不写分区
            df.write.parquet(save_path)
        elif not if_path_existed and partitionkey in df.columns:
            # 不存在表，但是写分区
            df.write.partitionBy(partitionkey).parquet(save_path)
        elif if_path_existed and partitionkey not in df.columns:
            # 存在表，但是不写分区
            # TODO：删除当日分区的数据，不知道spark是否或者parquet是否支持
            df.write.mode('append').parquet(save_path)
        else:
            # 存在表，写分区
            # 检测分区是否存在
            # TODO：删除当日分区的数据
            df.write.partitionBy(partitionkey).mode('append').parquet(save_path)

    @staticmethod
    def pipeline_v1(self, result, config, schema):
        # TODO
        # columns从哪里来? config放在哪里？ 可以放在yaml中，单独一个字典
        # 是否可以利用sql动态得到columns 
        # 如何生成schema
        # result = 
        pass

class MySQLClient(Client):
    def __init__(self, host, user, password, database, sql):
        self.host = host
        self.user = user
        self.password = password
        self.db = database
        self.sql = sql
    
    # @staticmethod
    # @retry(max_retries=3, retry_delay=5, exception_to_check=Exception)
    # def _create_connector(host, user, password):
    #     """
    #     返回一个mysql连接对象
    #     """
    #     try:
    #         client = pymysql.connect(host=host, user=user, password=password)
    #     except Exception as e:
    #         print(f'connect mysql failed, error: {e}')
    #         return None
    #     else:
    #         return client
    
    # @staticmethod
    # @retry(max_retries=3, retry_delay=5, exception_to_check=Exception)
    # def execute(self, connection, sql):
    #     """
    #     执行查询成功, 创建_result存储结果并返回;
    #     否则, 返回None
    #     """
    #     try:
    #         with connection.cursor() as cursor:
    #             cursor.execute(sql)
    #             self._result = cursor.fetchall()
    #         return self._result
    #     except Exception as e:
    #         print('execute sql failed, error: ', e)
    #         return None
    
    @staticmethod
    @contextlib.contextmanager
    @retry(max_retries=3, retry_delay=5, type='MySQLClient Connect...', exception_to_check=Exception)
    def mysql_client(host, user, password, database, cursorclass=None):
        connection = None
        try:
            if database != 'na':
                if not cursorclass:
                    connection = pymysql.connect(
                        host=host,
                        user=user,
                        password=password,
                        database=database
                    )
                else:
                    connection = pymysql.connect(
                        host=host,
                        user=user,
                        password=password,
                        database=database,
                        cursorclass=cursorclass
                    )
            else:
                print(f'database: {database}')
                if not cursorclass:
                    connection = pymysql.connect(
                            host=host,
                            user=user,
                            password=password
                    )
                else:
                    connection = pymysql.connect(
                            host=host,
                            user=user,
                            password=password,
                            cursorclass=cursorclass
                    )
            yield connection
        except Exception as e:
            print('Failed to connect to MySQL, error:', e)
            if connection is not None:
                connection.close()
        finally:
            if connection is not None and connection.open:
                connection.close()

    @staticmethod
    @retry(max_retries=3, retry_delay=5, type='MySQLClient Executing Query.....', exception_to_check=Exception)
    def execute(host, user, password, db, sql):
        """
        执行查询成功, 创建_result存储结果并返回;
        否则, 返回None
        """
        try:
            with MySQLClient.mysql_client(host, user, password, db) as connection: 
                print('Connection....initializing....')
                with connection.cursor() as cursor:
                    cursor.execute(sql)
                    result = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    print('result fetching successful......')
                    return result, columns
        except Exception as e:
            print(f'execute sql failed, error: {e}')
            return None, None
     
    # 接单   
    # @staticmethod
    # @retry(max_retries=3, retry_delay=5, type='MySQLClient Executing Query.....', exception_to_check=Exception)
    # def execute_batch(host, user, password, db, sql, batch_size):
    #     """
    #     执行查询成功, 创建_result存储结果并返回;
    #     否则, 返回None
    #     """
    #     try:
    #         with MySQLClient.mysql_client(host, user, password, db) as connection: 
    #             print('Connection....initializing....')
    #             with connection.cursor() as cursor:
    #                 cursor.execute(sql)
    #                 columns = [desc[0] for desc in cursor.description]
    #                 while True:
    #                     rows = cursor.fetchmany(batch_size)
    #                     if not rows:
    #                         break
    #                     print('fetching batches from db......')
    #                     yield rows, columns
    #     except Exception as e:
    #         print(f'execute sql failed, error: {e}')
    #         return None, None
        
    @staticmethod
    @retry(max_retries=3, retry_delay=5, type='MySQLClient Executing Query.....', exception_to_check=Exception)
    def execute_batch(host, user, password, db, sql, batch_size):
        """
        执行查询成功, 创建_result存储结果并返回;
        否则, 返回None
        """
        try:
            with MySQLClient.mysql_client(host, user, password, db, cursorclass=pymysql.cursors.SSCursor) as connection: 
                print('Connection....initializing....')
                with connection.cursor() as cursor:
                    cursor.execute(sql)
                    columns = [desc[0] for desc in cursor.description]
                    while True:
                        rows = cursor.fetchmany(batch_size)
                        if not rows:
                            break
                        print('fetching batches from db......')
                        yield rows, columns
        except Exception as e:
            print(f'execute sql failed, error: {e}')
            return None, None
    
class ClickhouseClient(Client):
    def __init__(self, host, port, user, password, sql):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.sql = sql

    # @staticmethod
    # @retry(max_retries=3, retry_delay=5, exception_to_check=Exception)
    # def _create_connector(host, port, user, password):
    #     try:
    #         client = CHClient(host=host, port=port, user=user, password=password)
    #     except Exception as e:
    #         print(f'connect clickhouse failed, error: {e}')
    #         return None
    #     else:
    #         return client
    
    # @staticmethod
    # @retry(max_retries=3, retry_delay=5, exception_to_check=Exception)
    # def execute(self, sql):
    #     try:
    #         with clickhouse_client(self.client) as client: 
    #             self._result = client.execute(sql)
    #         return self._result
    #     except Exception as e:
    #         print('execute sql failed, error: ', e)
    #         return None
    
    @staticmethod
    @contextlib.contextmanager
    @retry(max_retries=3, retry_delay=5, type='Clickhouse Client Connect...', exception_to_check=Exception)
    def ch_client(host, port, user, password):
        client = None
        try:
            client = CHClient(host=host, port=port, user=user, password=password)
            yield client
        except Exception as e:
            print('Failed to connect to ClickHouse, error:', e)
            if client is not None:
                try:
                    client.disconnect()
                except Exception as disconnect_error:
                    print('Failed to disconnect the ClickHouse client, error:', disconnect_error)
        finally:
            if client is not None:
                try:
                    client.disconnect()
                except Exception as disconnect_error:
                    print('Failed to disconnect the ClickHouse client, error:', disconnect_error)


    @staticmethod
    @retry(max_retries=3, retry_delay=5, type='staticmethod: executing query' ,exception_to_check=Exception)
    def execute(self, sql, host, port, user, password):
        try:
            with ClickhouseClient.ch_client(host, port, user, password) as client:
                self._result = client.execute(sql)
            return self._result
        except Exception as e:
            print('execute sql failed, error: ', e)
            return None
            
    @staticmethod
    @retry(max_retries=3, retry_delay=5, type='staticmethod: get_column_names' ,exception_to_check=Exception)
    def get_column_names(client, sql):
        """
        获取列名
        """
        try:
            print(f'sql : {sql + " LIMIT 1"}')
            result = client.execute(sql + " LIMIT 1", with_column_types=True)
            print(f'result: {result}')
            columns = [col[0] for col in result[1]]
            # print(f'columns: {columns}')
            return columns
        except Exception as e:
            print(f'Failed to get column names, error: {e}')
            return None


    
    @staticmethod
    @retry(max_retries=3, retry_delay=5, type='MySQLClient Executing Query.....', exception_to_check=Exception)
    def execute_batch(sql, host, port, user, password, batch_size):
        """
        执行查询成功, 创建_result存储结果并返回;
        否则, 返回None
        """
        
        try:
            with ClickhouseClient.ch_client(host, port, user, password) as client: 
                print('Connection....initializing....')
                
                # columns = ClickhouseClient.get_column_names(client, sql)
                # if not columns:
                #     return None, None
                print('fetching............')
                result = client.execute_iter(sql, with_column_types=True)
                print(f'sql = {sql}')
                # columns = [col[0] for col in result.columns_with_types]
                print('--------------------------------')
                print(f'fetched result: {result}')
                print('--------------------------------')
                print('fetched............')
                rows = []
                types = []
                count = 0
                columns_with_types = None
                
                
                for row in result:
                    if columns_with_types is None:
                        columns_with_types = row
                        columns = [col[0] for col in columns_with_types]
                        types = [col[1] for col in columns_with_types]
                        print('Columns and types:', columns_with_types)
                        print('columns:', columns)
                        print('types:', types)
                        print('--------------------------------')
                    else:
                        rows.append(row)
                        count += 1
                        if count == batch_size:
                            print('新迭代')
                            yield rows, columns, types
                            rows = []
                            count = 0
                if rows:
                    yield rows, columns, types
        except Exception as e:
            print(f'execute sql failed, error: {e}')
            return None, None, None