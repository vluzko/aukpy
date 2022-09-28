from pathlib import Path
from os import getenv


DATA_HOME = Path(getenv("XDG_DATA_HOME", Path.home() / ".local" / "share" / "aukpy"))
PACKAGE_DATA = Path(__file__).parent / "data"
TAXONOMY_CSV = PACKAGE_DATA / "taxonomy.csv"
BCR_CODES = PACKAGE_DATA / "bcr_codes.tsv"
IBA_CODES = PACKAGE_DATA / "iba_codes.tsv"
USFWS_CODES = PACKAGE_DATA / "usfws_codes.tsv"
