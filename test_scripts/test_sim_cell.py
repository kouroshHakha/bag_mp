import time
from bag.io import read_yaml
from bag_mp.src.bag_mp.core import BagMP
from bag_mp.src.bag_mp.client_wrapper import (
    synchronize, get_results
)

IOFORMAT = 'yaml'
if __name__ == '__main__':
    s = time.time()
    f = BagMP(processes=True, verbose=True)
    njobs = 3
    job_futures = []
    sch_future = None
    for i in range(njobs):
        specs = read_yaml(f'specs_gen/bag_mp/DTSA_sim/DTSA{i}.yaml')
        specs['impl_cell'] = f'{specs["impl_cell"]}_{i}'
        specs['impl_lib'] = f'{specs["impl_lib"]}_{i}'
        sim_results = f.sim_cell(specs, gen_cell=True, gen_wrapper=True, gen_tb=True, run_sim=True,
                                 bag_script='BAG2', io_format=IOFORMAT)
        job_futures.append(sim_results)

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
