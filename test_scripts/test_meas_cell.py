import time
from bag_mp.src.bag_mp.file import Yaml
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
        specs = Yaml.load(f'specs_gen/bag_mp/DTSA_meas/DTSA{i}.yaml')
        specs['impl_cell'] = f'{specs["impl_cell"]}_{i}'
        specs['impl_lib'] = f'{specs["impl_lib"]}_{i}'
        meas_results = f.meas_cell(specs, gen_cell=False, gen_wrapper=True, gen_tb=True,
                                   run_sim=True,
                                   bag_id='BAG2', io_format=IOFORMAT)
        job_futures.append(meas_results)

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
