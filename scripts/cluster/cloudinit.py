# Use this to generate a cloud-init-config.yaml file as suggested in https://cloudprovider.dask.org/en/latest/packer.html#ec2cluster-with-cloud-init
# Usage: ~/miniconda3/envs/cloudprovider/bin/python scripts/cluster/cloudinit.py
import yaml
from dask_cloudprovider.gcp import GCPCluster

ENV_CONDA_PATH = "envs/gwas.yaml"
ENV_VAR_PATH = "config/dask/cloudprovider.sh"

if __name__ == "__main__":
    with open(ENV_CONDA_PATH, "r") as f:
        deps = yaml.safe_load(f)["dependencies"]
        pip = " ".join(deps[-1]["pip"])
        conda = " ".join([d for d in deps if isinstance(d, str) and d != "pip"])
        conda += " -c conda-forge"

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
    cloud_init_config = GCPCluster.get_cloud_init(
        docker_image=docker_image,
        env_vars=dict(
            PUBLIC_INGRESS=False,
            EXTRA_CONDA_PACKAGES=f'"{conda}"',
            EXTRA_PIP_PACKAGES=f'"{pip}"',
        ),
    )
    print(cloud_init_config)
