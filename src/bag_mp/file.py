from typing import Dict, Any
import os
import string

import pickle
import yaml


class Pickle:
    """
    A global class for reading and writing Pickle format.
    """
    @staticmethod
    def save(obj: Any, file, **kwargs) -> None:
        with open(file, 'wb') as f:
            pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def load(file, **kwargs) -> Any:
        with open(file, 'rb') as f:
            return pickle.load(f)


class Yaml:
    """
    A global class for reading and writing yaml format
    For backward compatibility some module functions may overlap with this.
    """
    @staticmethod
    def save(obj: Any, file, **kwargs) -> None:
        with open(file, 'w') as f:
            yaml.dump(obj, f)

    @staticmethod
    def load(file, **kwargs) -> Any:
        with open(file, 'r') as f:
            return yaml.load(f, Loader=yaml.Loader)

    @staticmethod
    def read_yaml_env(file) -> Dict[str, Any]:
        """Parse YAML file with environment variable substitution.

        Parameters
        ----------
        file : os.Pathlike
            yaml file name.

        Returns
        -------
        table : Dict[str, Any]
            the yaml file as a dictionary.
        """
        content = read_file(file)
        # substitute environment variables
        content = string.Template(content).substitute(os.environ)
        return yaml.load(content, Loader=yaml.Loader)


def read_file(fname) -> str:
    """Read the given file and return content as string.

    Parameters
    ----------
    fname : os.Pathlike
        the file name.

    Returns
    -------
    content : unicode
        the content as a unicode string.
    """
    with open(fname, 'r') as f:
        content = f.read()
    return content
