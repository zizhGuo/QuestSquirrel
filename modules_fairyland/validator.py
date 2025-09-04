from abc import abstractmethod

class Validator:
    def __init__(self, name):
        self.data = {}
        self.name = f"_{name}" or "unassigned"

    def __get__(self, obj, objtype=None):
        return getattr(obj, self.name)

    def __set__(self, obj, value):
        self.validate(value)
        # self.data[obj] = value
        setattr(obj, self.name, value)

    @abstractmethod
    def validate(self, value):
        pass

class DictOfDict(Validator):
    def __init__(self, name):
        super(DictOfDict, self).__init__(name)

    def validate(self, value):
        print('enter validate')
        if not isinstance(value, dict):
            raise TypeError(f'{self.name} descriptor set value Failure. kwargs is not a dict: {value}')
        for k, subdict in value.items():
            if not isinstance(subdict, dict):
                raise TypeError(f'{self.name} descriptor set value Failure. subdict is not a dict: {subdict}')

class RequireKeyDictOfDictValidator(DictOfDict):
    def __init__(self, name, required_keys):
        super().__init__(name)
        self.required_keys = required_keys

    def validate(self, value):
        super().validate(value)
        print('required_keys: ', self.required_keys)

        for reqk in self.required_keys:
            if reqk not in value:
                raise ValueError(f'{self.name} descriptor set value Failure. required_keys: {self.required_keys}')

class Test:
    valid_keys = ['a']
    t_params = RequireKeyDictOfDictValidator('t_params', valid_keys)

    def __init__(self) -> None:
        try:
            self.t_params = {'a': {'b': 1}}
        except Exception as e:
            print(e)
    
    def print(self):
        print(1+1)
        print(self.t_params)

# t = Test({'c': {'b': 1}})
# t = Test()
# t.print()