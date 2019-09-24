import os
from pathlib import Path
import subprocess
import pickle
import operator as op
from dask.distributed import get_client, wait
from dask.distributed import Future
import time

from bag.util.immutable import to_immutable


def save(obj, file):
    with open(file, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def load(file):
    with open(file, 'rb') as f:
        return pickle.load(f)


class FutureWrapper:

    def __init__(self, future: Future):
        self._future = future

    @property
    def future(self):
        return self._future

    @property
    def executor(self):
        return self._future.executor

    @property
    def status(self):
        return self._future.status

    def done(self):
        return self._future.done()

    def result(self, timeout=None):
        return self._future.result(timeout)

    def exception(self, timeout=None, **kwargs):
        return self._future.exception(timeout, **kwargs)

    def add_done_callback(self, fn):
        return self._future.add_done_callback(fn)

    def cancel(self, **kwargs):
        return self._future.cancel(**kwargs)

    def retry(self, **kwargs):
        return self._future.retry(**kwargs)

    def cancelled(self):
        return self._future.cancelled()

    def traceback(self, timeout=None, **kwargs):
        return self._future.traceback(timeout, **kwargs)

    @property
    def type(self):
        return self._future.type

    def release(self, _in_destructor=False):
        return self._future.release(_in_destructor)

    def __getstate__(self):
        return self._future.__getstate__()

    def __setstate__(self, state):
        return self._future.__setstate__(state)

    def __del__(self):
        return self._future.__del__()

    def __repr__(self):
        return self._future.__repr__()

    def _repr_html_(self):
        return self._future.__repr__()

    def __await__(self):
        return self._future.__await__()

    def __getitem__(self, item):
        client = get_client()
        new_fut = client.submit(op.getitem, self._future, item)
        return FutureWrapper(new_fut)


def while_loop(cond, body, loop_vars):
    """
    Parameters
    ----------
    cond: Callable
        a callable returning a boolean, arguments are all loop_vars,
        should be serializable
    body: Callable
        a callable returning anything, arguments are all loop_vars, should
        be serializable
    loop_vars:
        all loop variables needed, can be Future or any other
        serializable object.

    Returns
    -------
    future: FutureWrapper
        a FutureWrapper object representing a updated loop_vars
    """
    client = get_client()

    def _while_task(c, b, lv=None):
        if not lv:
            lv = []
        updated_lv = lv
        while c(updated_lv):
            updated_lv = b(updated_lv)
            if updated_lv is None:
                updated_lv = []

        return updated_lv
    future = client.submit(_while_task, cond, body, loop_vars)
    return FutureWrapper(future)


def for_loop(iterable, body, loop_vars=None, enumerate=False):
    """

    Parameters
    ----------
    iterable:
        Iterable FutureWrapper/object to iterate over
    body:
        a callable returning updated loop_vars, arguments are items in iterable
        and all loop_vars. body(x, loop_vars), where x is an instances of
        elements in the iterator and loop_vars are all loop variables.
        method should be serializable.
    loop_vars:
        all loop variables needed, can be Future or any other
        serializable object.
    enumerate:
        True to use for x in enumerate(iterable) instead of x in iterable

    Returns
    -------
        a FutureWrapper object representing a updated loop_vars
    """
    client = get_client()

    def _for_task(iterable, b, lv=None):
        if not lv:
            lv = []
        updated_loop_vars = lv
        for x in iterable:
            updated_loop_vars = b(x, updated_loop_vars)
            if updated_loop_vars is None:
                updated_loop_vars = []

        return updated_loop_vars

    if enumerate:
        iterable = client.submit(enumerate, iterable)
    future = client.submit(_for_task, iterable, body, loop_vars)
    return FutureWrapper(future)


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
        open_mode = 'w+'
        if log_file is None:
            log_file = self.get_log_fname(tmp_file)
            open_mode = 'w'
        with open(log_file, open_mode) as log_f:
            exit_code = subprocess.call(cmd, stdout=log_f)
        if exit_code != 0:
            print(f'[info] process faild. log: {log_file}')
            raise SystemError('python subprocess failed')
        return log_file

    def _gen_cell(self, specs, dep=None, gen_lay=False, gen_sch=False,
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

    def gen_cell(self, specs, dep=None, gen_lay=False, gen_sch=False,
                 run_lvs=False, run_rcx=False, log_file=None):
        client = get_client()
        fut = client.submit(self._gen_cell, specs, dep=dep, gen_lay=gen_lay,
                            gen_sch=gen_sch, run_lvs=run_lvs, run_rcx=run_rcx,
                            log_file=log_file)
        return FutureWrapper(fut)

    def sim_cell(self):
        pass

    def design_cell(self):
        pass


class Titerator:
    def __init__(self, T):
        self._index = 0
        self.T = iter(T.list)

    def __next__(self):
        print('in next')
        return next(self.T)


class Titer:

    def __init__(self, list):
        self.list = list

    def __iter__(self):
        print('in iter')
        return Titerator(T)

    def __getitem__(self, item):
        return self.list[item]


if __name__ == '__main__':

    def f(a, b):
        ans = []
        for a_item, b_item in zip(a, b):
           ans.append(a_item + b_item)
        return ans

    def g(a, b):
        ans = []
        for a_item, b_item in zip(a, b):
           ans.append(a_item * b_item)
        return ans

    def z(a, b):
        ans = []
        for a_item, b_item in zip(a, b):
            ans.append(a_item * b_item)
        return ans

    a = [1, 2, 3]
    b = [10, 11, 12]
    from dask.distributed import Client
    client = Client(processes=False)
    f_ans = client.submit(f, a, b)
    g_ans = client.submit(g, b, f_ans)

    fut = for_loop(f_ans, lambda x, lv: print(x))
    print(fut.result())
    import pdb
    pdb.set_trace()

