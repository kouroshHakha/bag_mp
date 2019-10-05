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
    def __init__(self, temp_fname, *args, **kwargs):
        self.template = self._get_template(temp_fname)
        # the project object used for running minimum executable tasks
        interactive = kwargs.pop('interactive', False)
        verbose = kwargs.pop('verbose', False)
        self.prj = BagMP(interactive=interactive, verbose=verbose)

    @staticmethod
    def get_results(results: List[FutureWrapper]) -> Any:
        synchronize(results)
        cleared_results = []
        for job_res in results:
            try:
                res = job_res.result()
                cleared_results.append(res)
            except SystemError:
                cleared_results.append(SystemError)
        return cleared_results

    @staticmethod
    def sync(results: Union[List[FutureWrapper], FutureWrapper]) -> Any:
        return synchronize(results)

    def render(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return self.template.render_yaml(params)

    @staticmethod
    def _get_template(fname: os.PathLike) -> EvalTemplate:
        return EvalTemplate(fname)

    @abc.abstractmethod
    def batch_evaluate(self, batch_of_designs: Sequence[Dict[str, Any]], sync=False) \
            -> Sequence[Any]:
        raise NotImplementedError
