# file writer
# 
# pure writer
# 
#
#
#
# 
# author: guozizhun
# date: 2024.06.14
import os

from tqdm import tqdm
import pyarrow as pa 
import pyarrow.parquet as pq
import shutil

import gc

import multiprocessing as mp

from client import ClickhouseClient, MySQLClient

import time

pyarrow_types = {
        'Int8': pa.int8(),
        'Int16': pa.int16(),
        'Int32': pa.int32(),
        'Int64': pa.int64(),
        'UInt8': pa.uint8(),
        'UInt16': pa.uint16(),
        'UInt32': pa.uint32(),
        'UInt64': pa.uint64(),
        'Float32': pa.float32(),
        'Float64': pa.float64(),
        'String': pa.string(),
        'FixedString': pa.binary(),
        'Date': pa.date32(),
        'DateTime': pa.timestamp('s'),
        'DateTime64': pa.timestamp('us'),
        'Array(Int8)': pa.list_(pa.int8()),
        'Array(Int16)': pa.list_(pa.int16()),
        'Array(Int32)': pa.list_(pa.int32()),
        'Array(Int64)': pa.list_(pa.int64()),
        'Array(UInt8)': pa.list_(pa.uint8()),
        'Array(UInt16)': pa.list_(pa.uint16()),
        'Array(UInt32)': pa.list_(pa.uint32()),
        'Array(UInt64)': pa.list_(pa.uint64()),
        'Array(Float32)': pa.list_(pa.float32()),
        'Array(Float64)': pa.list_(pa.float64()),
        'Array(String)': pa.list_(pa.string())
}


class ParquetWriter:
    def __init__(self, *args, **kwargs):
        """
        basecally no use for now
        """
        pass

    @staticmethod
    def remove_partitions(output_dir, partition_cols, table):
        partition_values = [table[column].unique().to_pylist() for column in partition_cols]
        partition_paths = [os.path.join(output_dir, *[f"{col}={val}" for col, val in zip(partition_cols, vals)]) 
                        for vals in zip(*partition_values)]
        
        for path in partition_paths:
            if os.path.exists(path):
                shutil.rmtree(path)

    @staticmethod
    def convert_columns_to_lowercase(columns):
        return [col.lower() for col in columns]

    @staticmethod
    def _data2table(data, columns, add_columns=None):
        columns = ParquetWriter.convert_columns_to_lowercase(columns)
        arrays = [pa.array([row[i] for row in data]) for i in range(len(columns))]
        batch = pa.RecordBatch.from_arrays(arrays, names=columns)
        table = pa.Table.from_batches([batch])
        if add_columns:
            for col_name, col_value in add_columns.items():
                new_column = pa.array([col_value] * len(data))
                table = table.append_column(col_name, new_column)
        return table

    def _data2table_ch(data, columns, types):
        columns = ParquetWriter.convert_columns_to_lowercase(columns)
        schema = pa.schema([(col, pyarrow_types[types[i]]) for i, col in enumerate(columns)])
        
        arrays = []
        for i, col in enumerate(columns):
            array = [row[i] for row in data]
            arrays.append(pa.array(array, type=pyarrow_types[types[i]]))
        
        table = pa.Table.from_arrays(arrays, schema=schema)
        return table

    @staticmethod
    def ch_pipeline_batch(host, port, user, password, sql, batch_size, partition_cols, output_dir):
        for i, batch_data in enumerate(tqdm(ClickhouseClient.execute_batch(sql, host, port, user, password, batch_size))):
            data, columns, types = batch_data[0], batch_data[1], batch_data[2]
            print(f'data batch top 10: {data[:10]}')
            print(f'columns: {columns}')
            print(f'types: {types}')
            print(f'batch_index: {i}')
            time.sleep(2)
            table = ParquetWriter._data2table_ch(data, columns, types)
            print(f'table : {table}')
            _file = 'data' + f'_{str(i)}' + ".parquet"
            ParquetWriter.write_parquet(table, partition_cols, output_dir, _file, i)
    
    @staticmethod
    def start_pipeline(config: dict):
        try:
            if config.get('dbtype') == 'ch':
                ParquetWriter.ch_pipeline_batch(config.get('host'), 
                                                config.get('port'), 
                                                config.get('user'), 
                                                config.get('password'), 
                                                config.get('sql'), 
                                                config.get('batch_size'), 
                                                [config.get('partition_cols')],
                                                  config.get('output_dir')
                )
            else:
                partition_arg = [config.get('partition_cols')] if config.get('partition_cols') else []
                ParquetWriter.pipeline_batch_produce(config.get('host'), 
                                                     config.get('user'), 
                                                     config.get('password'), 
                                                     config.get('database'), 
                                                     config.get('sql'), 
                                                     config.get('batch_size'), 
                                                     partition_arg, 
                                                     config.get('add_columns'), 
                                                     config.get('output_dir')
            )
        except Exception as e:
            print(f'Run start_pipeline failed, {e}')
        else:
            print('start_pipeline exit')

    @staticmethod
    def pipeline_batch(host, user, password, database, sql, batch_size, partition_cols, output_dir, file):
        for i, batch_data in enumerate(tqdm(MySQLClient.execute_batch(host, user, password, database, sql, batch_size))):
            if i == 0:
                data, columns = batch_data[0], batch_data[1]
            data = batch_data[0]
            print(f'batch_index: {i}')
            time.sleep(2)
            table = ParquetWriter._data2table(data, columns)

            _file = file + f'_{str(i)}' + ".parquet"
            ParquetWriter.write_parquet(table, partition_cols, output_dir, _file, i)

    
    @staticmethod
    def pipeline_batch_produce(host, user, password, database, sql, batch_size, partition_cols, add_columns, output_dir):
        def producer_task(unused_queue, used_queue, data_dict, stop_event):
            # nonlocal queue_lock
            nonlocal flag
            # nonlocal flag_lock
            nonlocal lock
            for i, batch_data in enumerate(tqdm(MySQLClient.execute_batch(host, user, password, database, sql, batch_size))):
                print(f'producing....... batch: {i}') 
                with lock:
                    idx = unused_queue.get() 
                    print(f'get idx {idx}')
                    print(f'produced batch: {i}')
                    data_dict[idx] = batch_data
                    flag.value = i
                    print(f'set flag value={flag.value}')
                    used_queue.put(idx) 
                    print(f'put idx: {idx}')
            stop_event.set() 
            print('stop_event set')
        
        def consumer_task(unused_queue, used_queue, data_dict, partition_cols, add_columns, output_dir, file):
            while not stop_event.is_set() or not used_queue.empty():
                nonlocal flag
                nonlocal lock
                
                with lock:
                    if not used_queue.empty():
                        idx = used_queue.get()
                    else:
                        idx = None
                    # if not used_queue.empty():
                    if idx is not None:
                        data, columns = data_dict[idx]
                        table = ParquetWriter._data2table(data, columns, add_columns)
                        current_i = flag.value
                        print(f'current_i: {current_i}')
                        _file = file + f'_{str(idx)}' + ".parquet"
                        print(f'all params: table={table}, partition_cols={partition_cols}, output_dir={output_dir}, _file={_file}')
                        ParquetWriter.write_parquet(table, partition_cols, output_dir, _file, current_i)
                        del data_dict[idx]
                        gc.collect()
                        unused_queue.put(idx)
                    else:
                        time.sleep(0.1) 
        file = 'data'
        num_workers = 4
        manager = mp.Manager()
        unused_queue = manager.Queue()
        used_queue = manager.Queue()
        flag = manager.Value('i', -1)
        data_dict = manager.dict()
        stop_event = manager.Event()
        lock = manager.Lock()
        
        for i in range(num_workers * 2): 
            unused_queue.put(i)
        
        producer = mp.Process(target=producer_task, args=(unused_queue, used_queue, data_dict, stop_event))

        producer.start()

        consumers = [
            mp.Process(target=consumer_task, args=(unused_queue, used_queue, data_dict, partition_cols, add_columns, output_dir, file))
            for _ in range(num_workers)
        ]
        for consumer in consumers:
            time.sleep(1)
            consumer.start()

        producer.join()
        for consumer in consumers:
            consumer.join()
    
    @staticmethod
    def write_parquet(table, partition_cols, output_dir, file, i):
        if partition_cols:
            print(f'i = : {i}')
            if os.path.exists(output_dir) and i == 0:
                ParquetWriter.remove_partitions(output_dir, partition_cols, table)
            pq.write_to_dataset(
                table,
                root_path=output_dir,
                partition_cols=partition_cols,
                existing_data_behavior='overwrite_or_ignore'
            )
        else:
            file_path = os.path.join(output_dir, file)
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            if os.path.exists(output_dir):
                if os.path.exists(file_path):
                    os.remove(file_path)
                with pq.ParquetWriter(file_path, table.schema, use_dictionary=False) as writer:
                    writer.write_table(table)
            else:
                os.makedirs(output_dir)
                pq.write_table(table, file_path)