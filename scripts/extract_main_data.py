"""UKB main dataset extraction functions"""
import logging
import logging.config
from pathlib import Path

import fire
from pyspark.sql import SparkSession

logging.config.fileConfig(Path(__file__).resolve().parents[1] / "log.ini")
logger = logging.getLogger(__name__)


# See here for a description of this resource and its associated fields:
# http://biobank.ctsu.ox.ac.uk/crystal/label.cgi?id=100313
SAMPLE_QC_COLS = [
    "eid",
    "x22000_0_0",  # Genotype measurement batch
    "x22007_0_0",  # Genotype measurement plate
    "x22008_0_0",  # Genotype measurement well
    "x22001_0_0",  # Genetic sex
    "x22021_0_0",  # Genetic kinship to other participants
    "x22006_0_0",  # Genetic ethnic grouping
    "x22019_0_0",  # Sex chromosome aneuploidy
    "x22027_0_0",  # Outliers for heterozygosity or missing rate
    "x22003_0_0",  # Heterozygosity
    "x22004_0_0",  # Heterozygosity, PCA corrected
    "x22005_0_0",  # Missingness
    "x22020_0_0",  # Used in genetic principal components
    "x22022_0_0",  # Sex inference X probe-intensity
    "x22023_0_0",  # Sex inference Y probe-intensity
    "x22025_0_0",  # Affymetrix quality control metric "Cluster.CR"
    "x22026_0_0",  # Affymetrix quality control metric "dQC"
    "x22024_0_0",  # DNA concentration
    "x22028_0_0",  # Use in phasing Chromosomes 1-22
    "x22029_0_0",  # Use in phasing Chromosome X
    "x22030_0_0",  # Use in phasing Chromosome XY
    # These two fields are missing in our data from some reason
    # "x22009_0_0", # Genetic principal components
    # "x22002_0_0", # CEL files
]


def sample_qc(input_path: str, output_path: str):
    """Extract sample QC data from main dataset"""
    logger.info(f"Extracting sample qc from {input_path} into {output_path}")
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.parquet(input_path)
    pdf = df[SAMPLE_QC_COLS].toPandas()
    logger.info("Sample QC info:")
    pdf.info()
    pdf.to_csv(output_path, sep="\t", index=False)


if __name__ == "__main__":
    fire.Fire()
