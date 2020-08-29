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
SAMPLE_QC_COLS = {
    "eid": "eid",
    "x22000_0_0": "genotype_measurement_batch",
    "x22007_0_0": "genotype_measurement_plate",
    "x22008_0_0": "genotype_measurement_well",
    "x22001_0_0": "genetic_sex",
    "x22021_0_0": "genetic_kinship_to_other_participants",
    "x22006_0_0": "genetic_ethnic_grouping",
    "x22019_0_0": "sex_chromosome_aneuploidy",
    "x22027_0_0": "outliers_for_heterozygosity_or_missing_rate",
    "x22003_0_0": "heterozygosity",
    "x22004_0_0": "heterozygosity_pca_corrected",
    "x22005_0_0": "missingness",
    "x22020_0_0": "used_in_genetic_principal_components",
    "x22022_0_0": "sex_inference_x_probe_intensity",
    "x22023_0_0": "sex_inference_y_probe_intensity",
    "x22025_0_0": "affymetrix_quality_control_metric_cluster_cr",
    "x22026_0_0": "affymetrix_quality_control_metric_dqc",
    "x22024_0_0": "dna_concentration",
    "x22028_0_0": "use_in_phasing_chromosomes_1_22",
    "x22029_0_0": "use_in_phasing_chromosome_x",
    "x22030_0_0": "use_in_phasing_chromosome_xy",
    # -----------------------------------------------------
    # Additional fields beyond resource but relevant for QC
    # see: https://github.com/atgu/ukbb_pan_ancestry/blob/master/reengineering_phenofile_neale_lab2.r
    "x21022_0_0": "age_at_recruitment",
    "x31_0_0": "sex",
    "x21000_0_0": "ethnic_background",
    # -----------------------------------------------------
    # PCs
    "x22009_0_1": "genetic_principal_component_01",
    "x22009_0_2": "genetic_principal_component_02",
    "x22009_0_3": "genetic_principal_component_03",
    "x22009_0_4": "genetic_principal_component_04",
    "x22009_0_5": "genetic_principal_component_05",
    "x22009_0_6": "genetic_principal_component_06",
    "x22009_0_7": "genetic_principal_component_07",
    "x22009_0_8": "genetic_principal_component_08",
    "x22009_0_9": "genetic_principal_component_09",
    "x22009_0_10": "genetic_principal_component_10",
    "x22009_0_11": "genetic_principal_component_11",
    "x22009_0_12": "genetic_principal_component_12",
    "x22009_0_13": "genetic_principal_component_13",
    "x22009_0_14": "genetic_principal_component_14",
    "x22009_0_15": "genetic_principal_component_15",
    "x22009_0_16": "genetic_principal_component_16",
    "x22009_0_17": "genetic_principal_component_17",
    "x22009_0_18": "genetic_principal_component_18",
    "x22009_0_19": "genetic_principal_component_19",
    "x22009_0_20": "genetic_principal_component_20",
    "x22009_0_21": "genetic_principal_component_21",
    "x22009_0_22": "genetic_principal_component_22",
    "x22009_0_23": "genetic_principal_component_23",
    "x22009_0_24": "genetic_principal_component_24",
    "x22009_0_25": "genetic_principal_component_25",
    "x22009_0_26": "genetic_principal_component_26",
    "x22009_0_27": "genetic_principal_component_27",
    "x22009_0_28": "genetic_principal_component_28",
    "x22009_0_29": "genetic_principal_component_29",
    "x22009_0_30": "genetic_principal_component_30",
    "x22009_0_31": "genetic_principal_component_31",
    "x22009_0_32": "genetic_principal_component_32",
    "x22009_0_33": "genetic_principal_component_33",
    "x22009_0_34": "genetic_principal_component_34",
    "x22009_0_35": "genetic_principal_component_35",
    "x22009_0_36": "genetic_principal_component_36",
    "x22009_0_37": "genetic_principal_component_37",
    "x22009_0_38": "genetic_principal_component_38",
    "x22009_0_39": "genetic_principal_component_39",
    "x22009_0_40": "genetic_principal_component_40",
}


def sample_qc(input_path: str, output_path: str):
    """Extract sample QC data from main dataset"""
    logger.info(f"Extracting sample qc from {input_path} into {output_path}")
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.parquet(input_path)
    pdf = df[list(SAMPLE_QC_COLS.keys())].toPandas()
    pdf = pdf.rename(columns=SAMPLE_QC_COLS)
    logger.info("Sample QC info:")
    pdf.info()
    pdf.to_csv(output_path, sep="\t", index=False)


if __name__ == "__main__":
    fire.Fire()
