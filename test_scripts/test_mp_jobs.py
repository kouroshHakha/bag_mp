
from bag.io import read_yaml
from bag_mp.dask_bag.core import BagMP
from bag_mp.dask_bag.client_wrapper import synchronize, get_results

if __name__ == '__main__':
    f = BagMP(processes=True)
    njobs = 3
    job_futures = []
    sch_future = None
    for i in range(njobs):
        specs = read_yaml(f'specs_gen/bag_advanced_examples/DTSA{i}.yaml')
        specs['impl_cell'] = f'{specs["impl_cell"]}_{i}'
        sch_future = f.gen_cell(specs, gen_lay=True, gen_sch=True,
                                dep=sch_future)
        lvs_rcx_log = f.gen_cell(specs, dep=sch_future, run_lvs=True,
                                 run_rcx=True, log_file=sch_future[1])
        job_futures.extend([lvs_rcx_log, sch_future])
    print(get_results(job_futures, errors='skip'))
