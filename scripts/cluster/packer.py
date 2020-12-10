# Script to generate a cloud-init-config.yaml file as suggested in https://cloudprovider.dask.org/en/latest/packer.html#ec2cluster-with-cloud-init
import json
import warnings

import fire
from dask_cloudprovider.gcp import GCPCluster

# Ignore:
# - RuntimeWarning: coroutine 'wait_for' was never awaited
# - RuntimeWarning: coroutine 'SpecCluster._close' was never awaited
warnings.filterwarnings("ignore", category=RuntimeWarning, message="coroutine")

ENV_VAR_PATH = "config/dask/cloudprovider.sh"
PACKER_CONFIG_TEMPLATE = "config/dask/packer-config-template.json"


def get_cloud_init_config():
    with open(ENV_VAR_PATH) as f:
        docker_image = None
        for line in f.readlines():
            if line.startswith("export"):
                k = line.split("=")[0].split(" ")[1]
                v = line.split("=")[1]
                if k == "DASK_CLOUDPROVIDER__GCP__DOCKER_IMAGE":
                    docker_image = v
        if not docker_image:
            raise ValueError(f'Failed to find docker image in file "{ENV_VAR_PATH}"')

    # Use dependencies fixed in conda environment and cloud provider env var files
    # For a similar example, see https://gist.github.com/jacobtomlinson/15404d5b032a9f91c9473d1a91e94c0a
    config = GCPCluster.get_cloud_init(
        docker_image=docker_image, bootstrap=True, env_vars=dict(PUBLIC_INGRESS=False),
    )
    return config


def create_cloud_init_config():
    print(get_cloud_init_config())


def create_packer_config():
    with open(PACKER_CONFIG_TEMPLATE) as f:
        config = json.load(f)
        config["builders"][0]["metadata"]["user-data"] = get_cloud_init_config()
    print(json.dumps(config))


if __name__ == "__main__":
    fire.Fire()
