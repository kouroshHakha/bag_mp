import os
from pathlib import Path
import subprocess
from dask.distributed import get_client
from bag.io.file import Pickle, Yaml

from bag.util.immutable import to_immutable
from bag_mp.dask_bag.client_wrapper import FutureWrapper, create_client

PROCESS_TIMEOUT = 120
BAG2_FRAMEWORK = os.environ.get('BAG2_framework', 'BAG_framework')
BAG3_FRAMEWORK = os.environ.get('BAG3_framework', 'BAG_framework')

gen_cell_scripts = {
    'BAG2': Path(BAG2_FRAMEWORK) / 'run_scripts' / 'gen_cell.py',
    'BAG3': Path(BAG3_FRAMEWORK) / 'run_scripts' / 'gen_cell.py',
}

meas_cell_scripts = {
    'BAG2': Path(BAG2_FRAMEWORK) / 'run_scripts' / 'meas_cell.py',
    'BAG3': Path(BAG3_FRAMEWORK) / 'run_scripts' / 'meas_cell.py',
}

io_cls_dict = {
    'pickle': Pickle,
    'yaml': Yaml,
}


class BagMP:
    def __init__(self, interactive=False, verbose=False, **kwargs) -> None:
        create_client(**kwargs)
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

    def get_log_fname(self, tmp_file):
        return tmp_file.parent / f'{tmp_file.stem}_log.log'

    def run_script(self, script_path, tmp_file, output_path, format, args,
                   log_file=None):
        if self.interactive:
            cmd = ['./start_bag.sh', '-i', str(script_path), str(tmp_file)]
            cmd += ['--dump', str(output_path), '--format', format]
            cmd += args
        else:
            cmd = ['./run_bag.sh', str(script_path), str(tmp_file)]
            cmd += ['--dump', str(output_path), '--format', format]
            cmd += args

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

    def _gen_cell(self, specs, dep, gen_lay, gen_sch, run_lvs, run_rcx,
                  log_file, bag_script, io_format, **kwargs):
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
        updated_log = self.run_script(gen_cell_scripts[bag_script],
                                      tmp_file,
                                      out_tmp_file,
                                      io_format,
                                      args,
                                      log_file=log_file)

        if gen_sch or gen_lay:
            # return sch_params
            return io_cls.load(out_tmp_file, **kwargs), updated_log
        if run_lvs or run_rcx:
            # return log in case of failiure
            log = io_cls.load(out_tmp_file, **kwargs).get('log', '')
            if log:
                raise ValueError(f'lvs/rcx failed, log: {log}')

    def gen_cell(self, specs, dep=None, gen_lay=False, gen_sch=False,
                 run_lvs=False, run_rcx=False, log_file=None,
                 bag_script='BAG2', io_format='yaml'):
        client = get_client()
        fut = client.submit(self._gen_cell, specs, dep=dep, gen_lay=gen_lay,
                            gen_sch=gen_sch, run_lvs=run_lvs, run_rcx=run_rcx,
                            log_file=log_file, bag_script=bag_script,
                            io_format=io_format)
        return FutureWrapper.from_future(fut)

    def meas_cell(self):
        pass

    def sim_cell(self):
        pass

    def design_cell(self):
        pass
