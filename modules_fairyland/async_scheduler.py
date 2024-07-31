import os
import asyncio
# from concurrent.futures import ProcessPoolExecutor

from typing import Callable, Any

from .async_tasks import Task, Tasks

import time
from functools import wraps

DEBUG = 1

def sync_timed(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        print(f"sync func {func.__name__} started")
        start = time.perf_counter()
        try:
            return func(*args, **kwargs)
        finally:
            end = time.perf_counter()
            total = end - start
            print(f"sync func {func.__name__} spent {total} seconds to finish")
    return wrapper

async def tracker(tasks: Tasks, applied_tasks: list, sec: int) -> None:
    while True:
        await asyncio.sleep(sec)
        if DEBUG:
            print('----------------------------------------')
            print("tarcking...tasks\n {}".format(tasks))
            print('----------------------------------------')
            print('\n')

        if tasks.all_done():
            print('All tasks are done')
            break

        # glance the applied tasks
        if DEBUG:
            print('----------------------------------------')
        if applied_tasks:
            for future in applied_tasks[:]:
                if future[1].done():
                    try:
                        result = future[1].result()
                        if DEBUG:
                            print(f"任务 {future[0]} 已完成，结果：{result}")
                        tasks.get_task_by_idx(future[0]).done()
                    except IndexError as e:
                        if DEBUG:
                            print(f"任务 {future[0]} 失败: tasks中没找到该task，异常：{e}")
                        tasks.get_task_by_idx(future[0]).exception()
                    except Exception as e:
                        if DEBUG:
                            print(f"任务 {future[0]} 失败，异常：{e}")
                        tasks.get_task_by_idx(future[0]).exception()
                    finally:
                        applied_tasks.remove(future)
                else:
                    if DEBUG:
                        print(f"任务 {future[0]} 正在运行")
        else:
            if DEBUG:
                print('没有任务在运行')
        if DEBUG:
            print('----------------------------------------')
            print('\n')

async def producer(tasks: Tasks, available_tasks: list, sec: int) -> None:
    while True:
        await asyncio.sleep(sec)
        if DEBUG:
            print('----------------------------------------')
            print("生产中...")
        
        # atomic operation
        for i, task in enumerate(tasks.get_available_task()):
            available_tasks.append(task)
            task.ready()
        if DEBUG:
            print(f'availabe tasks: {available_tasks}')
            print('----------------------------------------')
            print('\n')

async def consumer(pool, available_tasks: list, applied_tasks: list, run: Callable, sec: int) -> None:
    # dumb consumer: just consume the available tasks
    while True:
        await asyncio.sleep(sec)
        if DEBUG:
            print('----------------------------------------')
            print("消费中...")
        loop = asyncio.get_event_loop()
        # atomic operation
        while available_tasks:
            task = available_tasks.pop(0)
            applied_tasks.append((task._idx, loop.run_in_executor(pool, run, task)))
            task.pending()
            if DEBUG:
                print('consuming: {}'.format(task))
        if DEBUG:
            print('----------------------------------------')
            print('\n')