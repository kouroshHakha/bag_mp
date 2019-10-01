from typing import cast

import time
from bag.io import read_yaml
from bag_mp.src.bag_mp.core import BagMP
from bag_mp.src.bag_mp.client_wrapper import (
    synchronize, get_results, FutureWrapper
)

IOFORMAT = 'yaml'
if __name__ == '__main__':
    s = time.time()
    f = BagMP(processes=True, verbose=True)
    njobs = 5
    job_futures = []
    sch_future = None
    for i in range(njobs):
        specs = read_yaml(f'specs_gen/bag_mp/DTSA_sim/DTSA{i}.yaml')
        specs['impl_cell'] = f'{specs["impl_cell"]}_{i}'
        specs['impl_lib'] = f'{specs["impl_lib"]}_{i}'
        lvs_rcx_log = f.gen_cell(specs, dep=None, gen_lay=True, gen_sch=True, run_lvs=True,
                                 run_rcx=True, log_file=None,
                                 bag_script='BAG2', io_format=IOFORMAT)
        lvs_done = cast(FutureWrapper, lvs_rcx_log == None)
        job_futures.append(lvs_done)

    synchronize(job_futures)
    results = []
    for job_res in job_futures:
        try:
            res = get_results(job_res)[0]
            results.append(res)
        except SystemError:
            results.append(None)
    print(results)
    print(f'{njobs} cicuits took: {time.time() - s:.5g} seconds.')
