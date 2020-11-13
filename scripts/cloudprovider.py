# Dask Cloud Provider REPL
#
# This is useful for creating clusters indepedent of the code that runs on them
# Example: python scripts/cloudprovider.py -- --interactive
#
import json
import os

import fire
from dask_cloudprovider.gcp.instances import GCPCluster

ENV_VAR_FILE = os.environ.get("ENV_VAR_FILE", "config/dask/env_vars.json")

with open(ENV_VAR_FILE, "r") as f:
    DEFAULT_ENV_VARS = json.load(f)


class CLI:
    def __init__(self):
        self.cluster = None

    def create(self, n_workers=None, env_vars=DEFAULT_ENV_VARS, name=None, **kwargs):
        self.cluster = GCPCluster(
            name=name, n_workers=n_workers, env_vars=env_vars, **kwargs
        )
        return self

    def _validate(self):
        if self.cluster is None:
            raise ValueError("Must create cluster first with `create` function")

    def adapt(self, min_workers, max_workers, interval="60s", wait_count=3):
        self._validate()
        self.cluster.adapt(
            minimum=min_workers,
            maximum=max_workers,
            interval=interval,
            wait_count=wait_count,
        )
        return self

    def scale(self, n_workers):
        self._validate()
        self.cluster.scale(n_workers)
        return self

    def shutdown(self):
        self._validate()
        self.cluster.close()
        return self

    def export_scheduler_info(self, path="/tmp/scheduler-info.txt"):
        self._validate()
        props = {
            "hostname": self.cluster.scheduler.name,
            "internal_ip": self.cluster.scheduler.internal_ip,
            "external_ip": self.cluster.scheduler.external_ip,
        }
        with open(path, "w") as f:
            for k, v in props.items():
                f.write(f"{k}={v}")
        return self

    def cluster(self):
        return self.cluster


if __name__ == "__main__":
    fire.Fire(CLI)
