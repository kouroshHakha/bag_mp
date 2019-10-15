from typing import Dict, Any, Callable

import os
from pathlib import Path
import subprocess
from dask.distributed import get_client

from .file import Pickle, Yaml
from .immutable import to_immutable

from .client_wrapper import FutureWrapper, create_client

PROCESS_TIMEOUT = 10000
BAG2_FRAMEWORK = os.environ.get('BAG2_FRAMEWORK', 'BAG_framework')
BAG3_FRAMEWORK = os.environ.get('BAG3_FRAMEWORK', 'BAG_framework')
BAG2_WORK_DIR = Path(BAG2_FRAMEWORK).parent
BAG3_WORK_DIR = Path(BAG3_FRAMEWORK).parent

config_dict = {
    'BAG2': {
        'work_dir': BAG2_WORK_DIR,
        'env_vars': Path(BAG2_FRAMEWORK).parent / '.cshrc',
        'framework': Path(BAG2_FRAMEWORK),
        'gen_cell': Path(BAG2_FRAMEWORK) / 'run_scripts' / 'gen_cell.py',
        'sim_cell': Path(BAG2_FRAMEWORK) / 'run_scripts' / 'sim_cell.py',
        'meas_cell': Path(BAG2_FRAMEWORK) / 'run_scripts' / 'meas_cell.py',
        'envs': {
            'BAG_WORK_DIR': BAG2_WORK_DIR,
            'BAG_TECH_CONFIG_DIR': BAG2_WORK_DIR/'GF14LPP',
            'BAG_CONFIG_PATH': BAG2_WORK_DIR/'bag_config.yaml',
        }
    },
    'BAG3': {
        'work_dir': Path(BAG3_FRAMEWORK).parent,
        'env_vars': Path(BAG3_FRAMEWORK).parent / '.cshrc',
        'framework': Path(BAG3_FRAMEWORK),
        'gen_cell': Path(BAG3_FRAMEWORK) / 'run_scripts' / 'gen_cell.py',
        'sim_cell': Path(BAG3_FRAMEWORK) / 'run_scripts' / 'sim_cell.py',
        'meas_cell': Path(BAG3_FRAMEWORK) / 'run_scripts' / 'meas_cell.py',
        'envs': {
            'BAG_WORK_DIR': BAG3_WORK_DIR,
            'BAG_TECH_CONFIG_DIR': BAG3_WORK_DIR / 'GF14LPP',
            'BAG_CONFIG_PATH': BAG3_WORK_DIR / 'bag_config.yaml',
        }
    }
}

io_cls_dict = {
    'pickle': Pickle,
    'yaml': Yaml,
}


class BagMP:
    def __init__(self, interactive=False, verbose=False, **kwargs) -> None:
        try:
            client = get_client()
            print(f'client loaded: {client}')
        except ValueError:
            create_client(**kwargs)
            print(f'client created: {get_client()}')
        self.bag_tmp_dir = os.environ.get('BAG_TEMP_DIR', None)
        self.interactive = interactive
        self.verbose = verbose

    def resolve_specs(self, specs, io_format, **kwargs):
        io_cls = io_cls_dict[io_format]
        tmp_dir = Path(self.bag_tmp_dir).resolve()
        const_specs = to_immutable(specs)
        tmp_file = tmp_dir / f'specs_{hash(const_specs)}.{io_format}'
        out_tmp_file = tmp_dir / f'{tmp_file.stem}_out.{io_format}'
        io_cls.save(specs, tmp_file, **kwargs)
        return tmp_file, out_tmp_file

    @staticmethod
    def get_log_fname(tmp_file):
        return tmp_file.parent / f'{tmp_file.stem}_log.log'

    def run_script(self, script_path, tmp_file, output_path, io_format, args, cwd, env,
                   log_file=None):
        cwd = Path(cwd).resolve()
        if self.interactive:
            cmd = ['./start_bag.sh', '-i', str(script_path), str(tmp_file)]
            cmd += ['--dump', str(output_path), '--format', io_format]
            cmd += args
        else:
            cmd = ['./run_bag.sh', str(script_path), str(tmp_file)]
            cmd += ['--dump', str(output_path), '--format', io_format]
            cmd += args

        open_mode = 'a'
        if log_file is None:
            log_file = self.get_log_fname(tmp_file)
            open_mode = 'w'
        with open(log_file, open_mode) as log_f:
            print(f'[running] {" ".join(cmd)}')
            if self.verbose:
                exit_code = subprocess.call(cmd, timeout=PROCESS_TIMEOUT, cwd=cwd, env=env)
            else:
                exit_code = subprocess.call(cmd, stdout=log_f, stderr=log_f,
                                            timeout=PROCESS_TIMEOUT, cwd=cwd, env=env)
        if exit_code != 0:
            print(f'[failure] {" ".join(cmd)}')
            print(f'log: {log_file}')
            raise SystemError('python subprocess failed')
        else:
            print(f'[success] {" ".join(cmd)}')
        return log_file

    def _get_env_vars(self, updated_envs: Dict[str, str]):
        envs = os.environ.copy()
        envs.update(updated_envs)
        return envs

    def _gen_cell(self, specs, dep, gen_lay, gen_sch, run_lvs, run_rcx,
                  log_file, bag_id, io_format, **kwargs):
        io_cls = io_cls_dict[io_format]
        tmp_file, out_tmp_file = self.resolve_specs(specs, io_format)
        args = []
        if not gen_lay:
            args.append('--no-lay')
        if not gen_sch:
            args.append('--no-sch')
        if run_lvs:
            args.append('-v')
        if run_rcx:
            args.append('-x')

        bag_config = config_dict[bag_id]
        cwd = bag_config['work_dir']
        envs = self._get_env_vars(bag_config['envs'])
        updated_log = self.run_script(bag_config['gen_cell'],
                                      tmp_file,
                                      out_tmp_file,
                                      io_format,
                                      args,
                                      cwd,
                                      env=envs,
                                      log_file=log_file)

        if gen_sch or gen_lay:
            # return sch_params
            return io_cls.load(out_tmp_file, **kwargs), updated_log

    def _sim_cell(self, specs, dep, gen_cell, gen_wrapper, gen_tb, load_results, extract,
                  run_sim, log_file, bag_id, io_format, **kwargs):
        io_cls = io_cls_dict[io_format]
        tmp_file, out_tmp_file = self.resolve_specs(specs, io_format)
        args = []
        if not gen_cell:
            args.append('--no-cell')
        if not gen_wrapper:
            args.append('--no-wrapper')
        if not gen_tb:
            args.append('--no-tb')
        if load_results:
            args.append('--load')
        if extract:
            args.append('-x')
        if not run_sim:
            args.append('--no-sim')

        bag_config = config_dict[bag_id]
        cwd = bag_config['work_dir']
        envs = self._get_env_vars(bag_config['envs'])
        updated_log = self.run_script(bag_config['sim_cell'],
                                      tmp_file,
                                      out_tmp_file,
                                      io_format,
                                      args,
                                      cwd,
                                      env=envs,
                                      log_file=log_file)

        if load_results or run_sim:
            # return sim results
            return io_cls.load(out_tmp_file, **kwargs), updated_log
        else:
            return updated_log

    def _meas_cell(self, specs, dep, gen_cell, gen_wrapper, gen_tb, load_results, extract,
                   run_sim, log_file, bag_id, io_format, **kwargs):
        io_cls = io_cls_dict[io_format]
        tmp_file, out_tmp_file = self.resolve_specs(specs, io_format)
        args = []
        if not gen_cell:
            args.append('--no-cell')
        if not gen_wrapper:
            args.append('--no-wrapper')
        if not gen_tb:
            args.append('--no-tb')
        if load_results:
            args.append('--load')
        if extract:
            args.append('-x')
        if not run_sim:
            args.append('--no-sim')

        bag_config = config_dict[bag_id]
        cwd = bag_config['work_dir']
        envs = self._get_env_vars(bag_config['envs'])
        updated_log = self.run_script(bag_config['meas_cell'],
                                      tmp_file,
                                      out_tmp_file,
                                      io_format,
                                      args,
                                      cwd,
                                      env=envs,
                                      log_file=log_file)

        if load_results or run_sim:
            # return meas results
            return io_cls.load(out_tmp_file, **kwargs), updated_log
        else:
            return updated_log

    def gen_cell(self, specs, dep=None, gen_lay=False, gen_sch=False,
                 run_lvs=False, run_rcx=False, log_file=None,
                 bag_id='BAG2', io_format='yaml'):
        client = get_client()
        fut = client.submit(self._gen_cell, specs, dep=dep, gen_lay=gen_lay,
                            gen_sch=gen_sch, run_lvs=run_lvs, run_rcx=run_rcx,
                            log_file=log_file, bag_id=bag_id,
                            io_format=io_format)
        return FutureWrapper.from_future(fut)

    def sim_cell(self, specs, dep=None, gen_cell=False, gen_wrapper=False,
                 gen_tb=False, load_results=False, extract=True, run_sim=False, log_file=None,
                 bag_id='BAG2', io_format='yaml'):
        """
        submits a simulation job to the queue of workers
        Parameters
        ----------
        specs: Dict[str, Any]
            specification dictionary
        dep: Any
            A dummy variable for specifying explicit dependencies. If during submission of a new
            job, it depends on the termination of another job, but explicit returned value from
            the former job, setting this variable can be useful.
        gen_cell: bool
            True to generate cell, default behavior does not generate cell.
        gen_wrapper: bool
            True to generate wrapper, default behavior does not generate wrapper.
        gen_tb: bool
            True to generate test bench, default behavior does not generate test bench.
        load_results: bool
            True to load the results and skip generation/simulation even when they are True.
        extract: bool
            False run schematic sims if simulation is True, default runs post-layout
            simulation.
        run_sim: bool
            True to run simulations, default behavior does not run simulation.
        log_file: bool
            The location of the log file, this is useful when you want a partial part of the
            graph to dump their log to the same place.
        bag_id:
            Look at the key words in sim_cell_scripts. Those are the valid key words.
        io_format
            yaml or pickle. It determines the interface format to external jobs.
        Returns
        -------
        FutureWrapper[Tuple[Any, Path]]
        The results of the simulation as well as the log file.
        """
        client = get_client()
        fut = client.submit(self._sim_cell, specs, dep=dep, gen_cell=gen_cell,
                            gen_wrapper=gen_wrapper,  gen_tb=gen_tb, load_results=load_results,
                            run_sim=run_sim, log_file=log_file, extract=extract,
                            bag_id=bag_id, io_format=io_format)
        return FutureWrapper.from_future(fut)

    def meas_cell(self, specs, dep=None, gen_cell=False, gen_wrapper=False,
                  gen_tb=False, load_results=False, extract=True, run_sim=False, log_file=None,
                  bag_id='BAG2', io_format='yaml'):
        client = get_client()
        fut = client.submit(self._meas_cell, specs, dep=dep, gen_cell=gen_cell,
                            gen_wrapper=gen_wrapper, gen_tb=gen_tb, load_results=load_results,
                            run_sim=run_sim, log_file=log_file, extract=extract,
                            bag_id=bag_id, io_format=io_format)
        return FutureWrapper.from_future(fut)

    def design_cell(self):
        pass

    @staticmethod
    def submit(func: Callable, *args, **kwargs) -> FutureWrapper:
        """
        Convenience function to submit arbitrary jobs to the client
        Parameters
        ----------
        func: Callable
            The function has to be serializable
        args:
            optional arg list, could be FutureWrappers or any other serializable object
        kwargs:
            optional keyword argument list, could be FutureWrappers or any other serializable object

        args and kwargs are parameters of the callable

        Returns
        -------
        results of the job as FutureWrapper objects
        """
        client = get_client()
        fut = client.submit(func, *args, **kwargs)
        return FutureWrapper.from_future(fut)
