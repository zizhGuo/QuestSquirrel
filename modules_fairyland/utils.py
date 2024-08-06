import os

def get_subdirectories(directory):
    """
    Return a list of subdirectory names in the given directory.

    :param directory: The path to the directory
    :return: A list of subdirectory names
    """
    try:
        subdirectories = [os.path.join(directory,name) for name in os.listdir(directory) if os.path.isdir(os.path.join(directory, name))]
        return subdirectories
    except FileNotFoundError:
        return f"Directory '{directory}' not found."
    except PermissionError:
        return f"Permission denied for directory '{directory}'."
    
def create_instance(mod, obj):
    """_summary_
        return the instance of the Customized class
    """
    import importlib
    module = importlib.import_module(mod)
    Class = getattr(module, obj)
    print('successfully import transform class.')
    return Class
