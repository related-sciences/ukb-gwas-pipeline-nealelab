"""Utilities for extracting resources from static, external sources"""
import logging
import logging.config
from pathlib import Path

import fire
import numpy as np
import pandas as pd

logging.config.fileConfig(Path(__file__).resolve().parents[1] / "log.ini")
logger = logging.getLogger(__name__)


def nlv3_sample_sets(input_path_european_samples: str, output_path: str):
    logger.info(
        f"Extracting sample sets [european={input_path_european_samples}] as {output_path}"
    )
    df = pd.read_csv(input_path_european_samples, sep="\t")
    df = df.rename(columns={"plate_name": "plate"})
    np.testing.assert_array_equal(df.columns, ["plate", "well"])
    df["in_european_samples"] = True
    # TODO: Outer join to other sample sets when files are available again
    df.to_csv(output_path, index=False)


if __name__ == "__main__":
    fire.Fire()
