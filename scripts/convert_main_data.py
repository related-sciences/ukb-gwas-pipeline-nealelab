"""UKB PLINK/BGEN to Zarr converter"""
import logging
import logging.config
from pathlib import Path

import fire
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

logging.config.fileConfig(Path(__file__).resolve().parents[1] / "log.ini")
logger = logging.getLogger(__name__)


def csv_to_parquet(input_path: str, output_path: str):
    """Convert primary UKB dataset CSV to Parquet"""
    logger.info(f"Converting csv at {input_path} to {output_path}")
    spark = SparkSession.builder.getOrCreate()

    # Fetch header only
    cols = pd.read_csv(input_path, nrows=1, sep=",", encoding="cp1252").columns.tolist()
    assert cols[0] == "eid", f'Expecting "eid" as first field, found "{cols[0]}"'
    # Convert field names for spark compatibility from
    # `{field_id}-{instance_index}.{array_index}` to
    # `x{field_id}_{instance_index}_{array_index}`
    # See: https://github.com/related-sciences/data-team/issues/22#issuecomment-613048099
    cols = [
        c if c == "eid" else "x" + c.replace("-", "_").replace(".", "_") for c in cols
    ]

    # Generate generic schema with string types (except sample id)
    schema = [StructField(cols[0], IntegerType())]
    schema += [StructField(c, StringType()) for c in cols[1:]]
    assert len(cols) == len(schema)
    schema = StructType(schema)

    # Read csv with no header
    df = spark.read.csv(
        input_path, sep=",", encoding="cp1252", header=False, schema=schema
    )
    df = df.filter(F.col("eid").isNotNull())
    logger.info(f"Number of partitions in result: {df.rdd.getNumPartitions()}")
    df.write.mode("overwrite").parquet(output_path, compression="snappy")

    logger.info("Done")


if __name__ == "__main__":
    fire.Fire()
