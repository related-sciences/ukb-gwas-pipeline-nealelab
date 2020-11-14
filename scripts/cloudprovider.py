# Dask Cloud Provider REPL
#
# This is useful for creating clusters indepedent of the code that runs on them
# Usage:
# python scripts/cloudprovider.py -- --interactive
#
import json
import os

import fire
from dask_cloudprovider.gcp.instances import GCPCluster

cluster = None

ENV_VAR_FILE = os.environ.get("ENV_VAR_FILE", "config/dask/env_vars.json")

with open(ENV_VAR_FILE, "r") as f:
    DEFAULT_ENV_VARS = json.load(f)


def _validate():
    if cluster is None:
        raise ValueError("Must create cluster first with `create` function")


def create(n_workers=None, env_vars=DEFAULT_ENV_VARS, name=None, **kwargs):
    global cluster
    cluster = GCPCluster(name=name, n_workers=n_workers, env_vars=env_vars, **kwargs)
    print("Cluster created")


def adapt(min_workers, max_workers, interval="60s", wait_count=3):
    _validate()
    cluster.adapt(
        minimum=min_workers,
        maximum=max_workers,
        interval=interval,
        wait_count=wait_count,
    )
    print(
        "Adaptive policy added to cluster (see https://docs.dask.org/en/latest/setup/adaptive.html)"
    )


def scale(n_workers):
    _validate()
    cluster.scale(n_workers)
    print("Cluster rescaled")


def shutdown():
    _validate()
    cluster.close()
    print("Cluster shutdown")


def export_scheduler_info(path="/tmp/scheduler-info.txt"):
    _validate()
    props = {
        "hostname": cluster.scheduler.name,
        "internal_ip": cluster.scheduler.internal_ip,
        "external_ip": cluster.scheduler.external_ip,
    }
    with open(path, "w") as f:
        for k, v in props.items():
            f.write(f"{k}={v}\n")
    print(f"Scheduler info exported to {path}")


def instance():
    return cluster


if __name__ == "__main__":
    fire.Fire()
