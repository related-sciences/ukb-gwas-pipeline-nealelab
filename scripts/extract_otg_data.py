"""V2D extraction functions"""
import logging
import logging.config
from pathlib import Path

import fire
from pyspark.sql import SparkSession

logging.config.fileConfig(Path(__file__).resolve().parents[1] / "log.ini")
logger = logging.getLogger(__name__)


def to_parquet(input_path: str, output_path: str, n_partitions: int):
    """Convert json data to parquet"""
    logger.info(f"Converting {input_path} json to parquet at {output_path}")
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.json(input_path)
    df = df.repartition(n_partitions)
    df.write.parquet(output_path)


if __name__ == "__main__":
    fire.Fire()
