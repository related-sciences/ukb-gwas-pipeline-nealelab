import os
import yaml
from pathlib import Path
from typing import List

PLINK_CONTIGS = os.environ.get("PLINK_CONTIGS", ' '.join(get_ukb_contig_names())).split()

BED_FMT = "ukb_cal_chr{contig}_v2.bed"
BIM_FMT = "ukb_snp_chr{contig}_v2.bim"
FAM_FMT = "ukb59384_cal_chr{contig}_v2_s488264.fam"

UKB_CONFIG_PATH = os.getenv('UKB_CONFIG_PATH', 'config.yml')
UKB_CONFIG_PATH = Path(UKB_CONFIG_PATH).resolve()

def get_ukb_config(path=UKB_CONFIG_PATH):
    with open(path) as fd:
        config = yaml.load(fd, Loader=yaml.FullLoader)
    return config

def get_ukb_contig_names() -> List[str]:
    config = get_ukb_config()
    return [c['name'] for c in config['contigs']]


def plink_bed_files() -> List[str]:
    