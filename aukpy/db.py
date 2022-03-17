import sqlite3
import csv

from pathlib import Path
from typing import Optional

from aukpy import config


HEADING = tuple(
    x.replace(" ", "_")
    for x in (
        "GLOBAL UNIQUE IDENTIFIER",
        "LAST EDITED DATE",
        "TAXONOMIC ORDER",
        "CATEGORY",
        "COMMON NAME",
        "SCIENTIFIC NAME",
        "SUBSPECIES COMMON NAME",
        "SUBSPECIES SCIENTIFIC NAME",
        "OBSERVATION COUNT",
        "BREEDING CODE",
        "BREEDING CATEGORY",
        "BEHAVIOR CODE",
        "AGE_SEX",
        "COUNTRY",
        "COUNTRY CODE",
        "STATE",
        "STATE CODE",
        "COUNTY",
        "COUNTY CODE",
        "IBA CODE",
        "BCR CODE",
        "USFWS CODE",
        "ATLAS BLOCK",
        "LOCALITY",
        "LOCALITY ID",
        "LOCALITY TYPE",
        "LATITUDE",
        "LONGITUDE",
        "OBSERVATION DATE",
        "TIME OBSERVATIONS STARTED",
        "OBSERVER ID",
        "SAMPLING EVENT IDENTIFIER",
        "PROTOCOL TYPE",
        "PROTOCOL CODE",
        "PROJECT CODE",
        "DURATION MINUTES",
        "EFFORT DISTANCE KM",
        "EFFORT AREA HA",
        "NUMBER OBSERVERS",
        "ALL SPECIES REPORTED",
        "GROUP IDENTIFIER",
        "HAS MEDIA",
        "APPROVED",
        "REVIEWED",
        "REASON",
        "TRIP COMMENTS",
        "SPECIES COMMENTS",
    )
)

HEADING_TYPES = {
    "GLOBAL_UNIQUE_IDENTIFIER": "text",
    "LAST_EDITED_DATE": "text",
    "TAXONOMIC_ORDER": "integer",
    "CATEGORY": "text",
    "COMMON_NAME": "text",
    "SCIENTIFIC_NAME": "text",
    "SUBSPECIES_COMMON_NAME": "text",
    "SUBSPECIES_SCIENTIFIC_NAME": "text",
    "OBSERVATION_COUNT": "integer",
    "BREEDING_CODE": "text",
    "BREEDING_CATEGORY": "text",
    "BEHAVIOR_CODE": "text",
    "AGE_SEX": "text",
    "COUNTRY": "text",
    "COUNTRY_CODE": "text",
    "STATE": "text",
    "STATE_CODE": "text",
    "COUNTY": "text",
    "COUNTY_CODE": "text",
    "IBA_CODE": "text",
    "BCR_CODE": "integer",
    "USFWS_CODE": "text",
    "ATLAS_BLOCK": "text",
    "LOCALITY": "text",
    "LOCALITY_ID": "text",
    "LOCALITY_TYPE": "text",
    "LATITUDE": "real",
    "LONGITUDE": "real",
    "OBSERVATION_DATE": "text",
    "TIME_OBSERVATIONS_STARTED": "text",
    "OBSERVER_ID": "text",
    "SAMPLING_EVENT_IDENTIFIER": "text",
    "PROTOCOL_TYPE": "text",
    "PROTOCOL_CODE": "text",
    "PROJECT_CODE": "text",
    "DURATION_MINUTES": "integer",
    "EFFORT_DISTANCE_KM": "real",
    "EFFORT_AREA_HA": "text",
    "NUMBER_OBSERVERS": "integer",
    "ALL_SPECIES_REPORTED": "integer",
    "GROUP_IDENTIFIER": "text",
    "HAS_MEDIA": "integer",
    "APPROVED": "integer",
    "REVIEWED": "integer",
    "REASON": "text",
    "TRIP_COMMENTS": "text",
    "SPECIES_COMMENTS": "text",
}


COUNTRY_TABLE_Q = """CREATE TABLE IF NOT EXISTS countries (
    id: integer PRIMARY KEY,
    name: text,
    code: text,
    UNIQUE(name, code)
);"""
STATE_TABLE_Q = "CREATE TABLE IF NOT EXISTS states (id: integer PRIMARY KEY, name: text, code: text, country_id integer, FOREIGN KEY (country_id) REFERENCES countries(id), UNIQUE(name, code));"
COUNTY_TABLE_Q = "CREATE TABLE IF NOT EXISTS states (id: integer PRIMARY KEY, name: text, code: text, state_id integer, FOREIGN KEY (state_id) REFERENCES states(id), UNIQUE(name, code));"
BCR_CODE_TABLE_Q = "CREATE TABLE IF NOT EXISTS bcrcodes (code: integer PRIMARY KEY, name: text);"
IBA_CODE_TABLE_Q = "CREATE TABLE IF NOT EXISTS ibacodes (code: integer PRIMARY KEY, name: text);"
SPECIES_TABLE_Q = "CREATE TABLE IF NOT EXISTS species (id: integer PRIMARY KEY, common_name: text, scientific_name: text, subspecies_common_name: text, subspecies_scientific_name: text);"


def build_db(input_path: Path, output_path: Optional[Path] = None):
    """Build a sqlite database from an input file

    Args:
        input_path: Path to the input dataset
        output_path: Desired path for the database. Defaults to {DATA_FOLDER}/{input_path.stem}.sqlite
    """
    if output_path is None:
        output_path = (config.DATA_HOME / input_path.stem).with_suffix(".sqlite")

    db = sqlite3.connect(":memory:")
    columns = ",\n\t".join((f"{c} {t}" for c, t in HEADING_TYPES.items()))
    query = f"CREATE TABLE IF NOT EXISTS ebird ({columns});"
    db.execute(query)
    parameters = ("?" for _ in HEADING)
    parameters_str = ", ".join(parameters)
    heading_str = ", ".join(HEADING)
    insert_query = f"INSERT INTO ebird ({heading_str}) VALUES ({parameters_str});"
    with open(input_path) as input_f:
        reader = csv.reader(input_f, delimiter="\t")
        # Skip the header line
        reader.__next__()
        for line in reader:
            db.execute(insert_query, line)
