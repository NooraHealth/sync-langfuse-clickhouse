import os
from pathlib import Path
import yaml

def get_params(source_name, params_path = 'params.yaml', envir = None):
    with open(params_path) as f:
        all_params = yaml.safe_load(f)

    if envir is None:
        github_ref_name = os.getenv('GITHUB_REF_NAME')
        envir = 'prod' if github_ref_name == 'main' else 'dev'

    print(f"env='{envir}' | source='{source_name}'")

    env_params = all_params[envir]
    params = env_params[source_name]
    return params
