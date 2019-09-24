from typing import Optional, Any, Dict, Type, TypeVar

import os
import string
import yaml
import importlib
from pathlib import Path
import subprocess
from dask.distributed import get_client, Client, wait
import pickle

from bag.io import read_file, read_yaml
from bag.layout.core import DummyTechInfo, TechInfo
from bag.util.immutable import to_immutable
from bag.design import ModuleDB


def save(obj, file):
    with open(file, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def load(file):
    with open(file, 'rb') as f:
        return pickle.load(f)


class BagProcess:

    def __init__(self, interactive=False) -> None:
        self.bag_tmp_dir = os.environ.get('BAG_TEMP_DIR', None)
        self.interactive = interactive

    def resolve_specs(self, specs):
        tmp_dir = Path(self.bag_tmp_dir).resolve()
        const_specs = to_immutable(specs)
        tmp_file = tmp_dir / f'specs_{hash(const_specs)}.pickle'
        out_tmp_file = tmp_dir / f'{tmp_file.stem}_out.pickle'
        save(specs, tmp_file)
        return tmp_file, out_tmp_file

    def get_log_fname(self, tmp_file):
        return tmp_file.parent / f'{tmp_file.stem}_log.log'

    def run_gen_cell_script(self, tmp_file, *args, log_file=None):
        parent = Path(__file__).parent.parent
        run_script = parent / Path('run_scripts/gen_cell.py')
        if self.interactive:
            cmd = ['./start_bag.sh', '-i', str(run_script), str(tmp_file)] + \
                  list(args)
        else:
            cmd = ['./run_bag.sh', str(run_script), str(tmp_file)] + list(args)

        print(f'[info] running python subprocecess {" ".join(cmd)}')
        open_mode = 'w'
        if log_file is None:
            log_file = self.get_log_fname(tmp_file)
            open_mode = 'w+'
        with open(log_file, open_mode) as log_f:
            exit_code = subprocess.call(cmd, stdout=log_f)
        if exit_code != 0:
            print(f'[info] process faild. log: {log_file}')
            raise SystemError('python subprocess failed')
        return log_file

    def gen_layout(self, specs, dep=None):
        tmp_file, out_tmp_file = self.resolve_specs(specs)
        self.run_gen_cell_script(tmp_file, '--no-sch')
        sch_params = load(out_tmp_file)
        return sch_params

    def gen_sch(self, specs, dep=None):
        tmp_file, _ = self.resolve_specs(specs)
        self.run_gen_cell_script(tmp_file, '--no-lay')

    def gen_cell(self, specs, dep=None, gen_lay=False, gen_sch=False,
                 run_lvs=False, run_rcx=False, log_file=None):
        tmp_file, out_tmp_file = self.resolve_specs(specs)
        args = []
        if not gen_lay:
            args.append('--no-lay')
        else:
            print('creating lay')
        if not gen_sch:
            args.append('--no-sch')
        else:
            print('creating sch')
        if run_lvs:
            print('running lvs')
            args.append('-v')
        if run_rcx:
            print('running rcx')
            args.append('-x')
        updated_log = self.run_gen_cell_script(tmp_file, *args,
                                               log_file=log_file)

        if gen_sch or gen_lay:
            # return sch_params
            return load(out_tmp_file), updated_log
        if run_lvs or run_rcx:
            # return log in case of failiure
            log = load(out_tmp_file).get('log', '')
            if log:
                raise ValueError(f'lvs/rcx failed, log: {log}')

    def run_lvs(self, specs, dep=None):
        tmp_file, out_tmp_file = self.resolve_specs(specs)
        self.run_gen_cell_script(tmp_file, '--no-sch', '--no-lay', '-v')
        log = load(out_tmp_file).get('log', None)
        if log is None:
            raise ValueError(f'lvs/rcx failed, log: {log}')

    def run_rcx(self, specs, dep=None):
        tmp_file, out_tmp_file = self.resolve_specs(specs)
        self.run_gen_cell_script(tmp_file, '--no-sch', '--no-lay', '-x')
        log = load(out_tmp_file).get('log', None)
        if log is None:
            raise ValueError(f'lvs/rcx failed, log: {log}')


if __name__ == '__main__':
    f = BagProcess()
    client = Client(processes=False)
    njobs = 1
    job_futures = []
    sch_done = None
    for i in range(njobs):
        specs = read_yaml(f'specs_gen/bag_advanced_examples/DTSA{i}.yaml')
        specs['impl_cell'] = f'{specs["impl_cell"]}_{i}'
        sch_params = client.submit(f.gen_cell, specs, dep=sch_done,
                                   gen_lay=True)
        specs['params'] = sch_params
        sch_done = client.submit(f.gen_cell, specs, dep=sch_params,
                                 gen_sch=True)
        lvs = client.submit(f.gen_cell, specs, dep=sch_done, run_lvs=True)
        job_futures.extend([sch_done, lvs])
    wait(job_futures)
    print(job_futures)