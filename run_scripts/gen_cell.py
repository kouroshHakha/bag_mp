import argparse
from bag.core import BagProject
from bag.io.file import read_yaml
from pathlib import Path
import pickle


def save(obj, file):
    with open(file, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def load(file):
    with open(file, 'rb') as f:
        return pickle.load(f)


def run_main(prj: BagProject,
             specs_fname: Path,
             no_sch: bool,
             no_lay: bool,
             lvs: bool,
             rcx: bool,
             use_cache: bool,
             save_cache: bool,
             prefix: str,
             suffix: str,):

    specs = load(str(specs_fname))

    results = prj.generate_cell(specs=specs,
                                gen_lay=not no_lay,
                                gen_sch=not no_sch,
                                run_lvs=lvs,
                                run_rcx=rcx,
                                use_cybagoa=True,
                                use_cache=use_cache,
                                save_cache=save_cache,
                                prefix=prefix,
                                suffix=suffix)

    if results is not None:
        out_tmp_file = specs_fname.parent / f'{specs_fname.stem}_out.pickle'
        save(results, out_tmp_file)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('specs_fname', help='specs yaml file')
    parser.add_argument('--no-sch', dest='no_sch', action='store_true', default=False,
                        help='skip schematic generation')
    parser.add_argument('--no-lay', dest='no_lay', action='store_true', default=False,
                        help='skip layout generation')
    parser.add_argument('-v', '--lvs', action='store_true', default=False, help='run lvs')
    parser.add_argument('-x', '--rcx', action='store_true', default=False, help='run rcx')
    parser.add_argument('--use-cache', dest='use_cache', action='store_true', default=False,
                        help='uses the cache in cache_dir')
    parser.add_argument('--save-cache', dest='save_cache', action='store_true', default=False,
                        help='updates design database stored in cache_dir')
    parser.add_argument('--pre', dest='prefix', default='',
                        help='prefix used to generate all the cells')
    parser.add_argument('--suf', dest='suffix', default='',
                        help='suffix used to generate all the cells')
    args = parser.parse_args()

    local_dict = locals()
    bprj = local_dict.get('bprj', BagProject())

    run_main(bprj,
             Path(args.specs_fname),
             args.no_sch,
             args.no_lay,
             args.lvs,
             args.rcx,
             args.use_cache,
             args.save_cache,
             args.prefix,
             args.suffix,)
