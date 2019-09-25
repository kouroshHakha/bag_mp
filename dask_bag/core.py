import os
from pathlib import Path
import subprocess
import pickle
from dask.distributed import get_client

from bag.util.immutable import to_immutable
from bag_mp.dask_bag.client_wrapper import FutureWrapper, create_client

PROCESS_TIMEOUT = 120


def save(obj, file):
    with open(file, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def load(file):
    with open(file, 'rb') as f:
        return pickle.load(f)


class BagMP:

    def __init__(self, interactive=False, verbose=False, **kwargs) -> None:
        create_client(**kwargs)
        self.bag_tmp_dir = os.environ.get('BAG_TEMP_DIR', None)
        self.interactive = interactive
        self.verbose = verbose

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

        open_mode = 'a'
        if log_file is None:
            log_file = self.get_log_fname(tmp_file)
            open_mode = 'w'
        with open(log_file, open_mode) as log_f:
            if self.verbose:
                exit_code = subprocess.call(cmd, timeout=PROCESS_TIMEOUT)
            else:
                exit_code = subprocess.call(cmd, stdout=log_f, stderr=log_f,
                                            timeout=PROCESS_TIMEOUT)
        if exit_code != 0:
            print(f'[failure] {" ".join(cmd)}')
            print(f'log: {log_file}')
            raise SystemError('python subprocess failed')
        else:
            print(f'[success] {" ".join(cmd)}')
        return log_file

    def _gen_cell(self, specs, dep=None, gen_lay=False, gen_sch=False,
                  run_lvs=False, run_rcx=False, log_file=None):
        tmp_file, out_tmp_file = self.resolve_specs(specs)
        args = []
        if not gen_lay:
            args.append('--no-lay')
        if not gen_sch:
            args.append('--no-sch')
        if run_lvs:
            args.append('-v')
        if run_rcx:
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

    def gen_cell(self, specs, dep=None, gen_lay=False, gen_sch=False,
                 run_lvs=False, run_rcx=False, log_file=None):
        client = get_client()
        fut = client.submit(self._gen_cell, specs, dep=dep, gen_lay=gen_lay,
                            gen_sch=gen_sch, run_lvs=run_lvs, run_rcx=run_rcx,
                            log_file=log_file)
        return FutureWrapper.from_future(fut)

    def sim_cell(self):
        pass

    def design_cell(self):
        pass
