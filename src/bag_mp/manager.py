from typing import Dict, Any, Sequence, List, Union

import abc
from .core import BagMP
from .client_wrapper import synchronize, FutureWrapper, get_results
from pathlib import Path
from .file import read_file
from jinja2 import Template
import yaml
import os


class EvalTemplate:
    def __init__(self, temp_path: os.PathLike):
        self._path: Path = Path(temp_path).resolve()
        self.content: str = read_file(self._path)
        self.jinja_temp = Template(self.content)

    def render_plain(self, params: Dict[str, Any]) -> str:
        return self.jinja_temp.render(**params)

    def render_yaml(self, params: Dict[str, Any]) -> Dict[str, Any]:
        yaml_content = self.render_plain(params)
        specs = yaml.load(yaml_content, Loader=yaml.FullLoader)
        return specs


class EvaluationManager(abc.ABC):
    def __init__(self, temp_fname):
        self.template = self._get_template(temp_fname)
        self.prj = BagMP()

    @staticmethod
    def get_results(results) -> Any:
        return get_results(results, errors='skip')

    @staticmethod
    def sync(results: Union[List[FutureWrapper], FutureWrapper]) -> Any:
        return synchronize(results)

    @staticmethod
    def _get_template(fname: os.PathLike) -> EvalTemplate:
        return EvalTemplate(fname)

    @abc.abstractmethod
    def batch_evaluate(self, batch_of_designs: Sequence[Dict[str, Any]], sync=False) \
            -> Sequence[Any]:
        raise NotImplementedError
