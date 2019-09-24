from dask.distributed import get_client, Client, wait

from bag.io import read_yaml
from bag_mp.dask_bag.core import BagProcess

if __name__ == '__main__':
    f = BagProcess()
    client = Client(processes=False)
    njobs = 1
    job_futures = []
    sch_done = None
    for i in range(njobs):
        specs = read_yaml(f'specs_gen/bag_advanced_examples/DTSA{i}.yaml')
        specs['impl_cell'] = f'{specs["impl_cell"]}_{i}'
        sch_params, log_file = f.gen_cell(specs, gen_lay=True, gen_sch=True,
                                          dep=sch_done)
        print(type(log_file))
        import pdb
        pdb.set_trace()
        sch_done = sch_params
        lvs_rcx_log = f.gen_cell(specs, dep=sch_done, run_lvs=True,
                                 run_rcx=True, log_file=log_file)
        job_futures.append(lvs_rcx_log)
    wait(job_futures)
    print(job_futures)