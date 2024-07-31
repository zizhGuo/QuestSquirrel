_NOT_STARTED = 'NOT_STARTED'
_READY = 'READY'
_PENDING = 'PENDING'
_FINISHED = 'FINISHED'
_CANCELLED = 'CANCELLED'
_EXCEPTION = 'EXCEPTION'

# new state represent not yet started

# def search_denpendecy_status(Task: object):
#     # check if all dependencies are finished
#     for dep in Task._dependencies:
#         if dep._status != _FINISHED:
#             return False
#     return True

class Tasks(dict):
    def get_available_task(self) -> list:
        # multiple times calling safe
        available_tasks = []
        for task_id, task in self.items():
            if task._status in [_CANCELLED, _EXCEPTION, _FINISHED, _PENDING]:
                continue
            if task._status==_READY \
                or (task.denpendecy_done(self) and task._status==_NOT_STARTED):
                available_tasks.append(task)
        return available_tasks

    def print_all_tasks_status(self):
        content = ''
        for task_id, task in self.items():
            # print(f'task {task_id} status: {task}')
            content += f'task {task_id} | {task}'
            content += '\n'
        return content
    
    def get_task_by_idx(self, idx: int) -> object:
        return self.get(idx, None)
    
    def all_done(self) -> bool:
        for task_id, task in self.items():
            if task._status not in (_FINISHED, _CANCELLED, _EXCEPTION):
                return False
        return True

    def __repr__(self) -> str:
        return self.print_all_tasks_status()
    
class Task:
    _idx = None
    _sql = None
    _query_instance = None
    _status = _NOT_STARTED
    _dependencies = []
    _coro = None

    def __init__(self, **kvs) -> None:
        if 'idx' in kvs:
            self._idx = kvs['idx']
        if 'sql' in kvs:
            self._sql = kvs['sql']
        if 'query_instance' in kvs:
            self._query_instance = kvs['query_instance']
        if 'dependencies' in kvs:
            self._dependencies = kvs['dependencies']

    def done(self) -> bool:
        assert self._status == _PENDING, f"Task {self._idx} is not pending"
        self._status = _FINISHED
        return self._status
    
    def exception(self):
        assert self._status == _PENDING, f"Task {self._idx} is not pending"
        self._status = _EXCEPTION
        return self._status

    def pending(self):
        if self._status == '_PENDING':
            raise Exception('Double ')
        assert self._status == _READY, f"Task {self._idx} is not READY"
        self._status = _PENDING
        return self._status

    def ready(self):
        assert self._status == _NOT_STARTED or _READY, f"Task {self._idx} is not NOT STARTED"
        self._status = _READY
        return self._status

    def denpendecy_done(self, Tasks: list) -> bool:
        # check if all dependencies are finished
        for dep_id in self._dependencies:
            if Tasks.get(dep_id, None) is None:
                return False
            dep = Tasks[dep_id]
            if dep._status != _FINISHED:
                return False
        return True

    def set_coro(self, coro):
        self._coro = coro

    def set_status(self, status: str) -> None:
        # not using it so far
        self._status = status

    def get_query_instance(self):
        return self._query_instance

    def __repr__(self) -> str:
        return f"Task {self._idx}; \
        STATUS: {self._status}; \
        DEPENDENCIES: {self._dependencies}; \
        OBJ: { self._query_instance is not None}"
