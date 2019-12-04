import os
import yaml
from sys import intern


class InterningLoader(yaml.SafeLoader):

    def intern_yaml_str(self, node):
        value = self.construct_scalar(node)
        return intern(value)

InterningLoader.add_constructor('tag:yaml.org,2002:str',
                                InterningLoader.intern_yaml_str)


def load_file(filepath):
    with open(filepath, 'r') as fr:
        return yaml.load(stream=fr, Loader=InterningLoader)


def save_file(filepath, data, **kwds):
    dirpath = os.path.dirname(filepath)
    os.makedirs(dirpath, exist_ok=True)
    with open(filepath, 'w') as fw:
        return yaml.safe_dump(data, stream=fw, **kwds)


#endfile
