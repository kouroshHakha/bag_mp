from bag_mp.src.bag_mp.manager import EvaluationManager
from pathlib import Path

if __name__ == '__main__':
    template = Path('specs_gen/bag_mp/DTSA_temps/dtsa.yaml')

    templates = {'dtsa': template}

    dtsa_manager = EvaluationManager(templates)