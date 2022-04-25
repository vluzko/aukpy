"""Tools for building a sqlite database containing the eBird dataset.
The full dataset is distributed as a ~400GB csv file, much of which is redundant.
We store it in a fully normalized sqlite file instead, which reduces the size of the data
and makes querying it much faster.
"""
import sqlite3
import csv

from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from aukpy import config


HEADINGS = tuple(
    x.replace(" ", "_").lower()
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


COUNTRY_HEADINGS = ('country', 'country_code', 'state', 'state_code', 'county', 'county_code')
BCRCODE_HEADINGS = ('bcr_code',)
IBACODE_HEADINGS = ('iba_code',)
SPECIES_HEADINGS = ('taxonomic_order', 'category', 'common_name', 'scientific_name', 'subspecies_common_name', 'subspecies_scientific_name')
OBSERVER_HEADINGS = ('observer_id', )
BREEDING_HEADINGS = ('breeding_code', 'breeding_category', 'behavior_code')
LOCALITY_HEADINGS = ('locality', 'locality_id', 'locality_type')
PROTOCOL_HEADINGS = ('protocol_type', 'protocol_code', 'project_code')

# Note: the order of headings in this tuple must be the same as the order of foreign keys in the observations table.
# In python > 3.7 tracking the order separately is unnecessary, but we handle it anyway
TABLE_ORDER = ('countries',
    'bcr_codes',
    'iba_codes',
    'species',
    'observers',
    'breeding',
    'localities',
    'protocols')
SEPARATE_TABLES = {
    'countries': COUNTRY_HEADINGS,
    'bcr_codes': BCRCODE_HEADINGS,
    'iba_codes': IBACODE_HEADINGS,
    'species': SPECIES_HEADINGS,
    'observers': OBSERVER_HEADINGS,
    'breeding': BREEDING_HEADINGS,
    'localities': LOCALITY_HEADINGS,
    'protocols': PROTOCOL_HEADINGS
}

heading_indices = {
    k: tuple(HEADINGS.index(heading) for heading in table) for k, table in SEPARATE_TABLES.items()
}

skip_indices = {x for indices in heading_indices.values() for x in indices}

# We could construct these, but since there's only a few it's better to just hardcode them
country_query = """INSERT OR IGNORE INTO countries (country_name, country_code, state_name, state_code, county_name, county_code)
VALUES(?, ?, ?, ?, ?, ?)"""

bcr_query = """INSERT OR IGNORE INTO bcrcodes (bcr_code)
VALUES(?)"""

iba_query = """INSERT OR IGNORE INTO ibacodes (iba_code)
VALUES(?)"""

species_query = """INSERT OR IGNORE INTO species (taxonomic_order, category, common_name, scientific_name, subspecies_common_name, subspecies_scientific_name)
VALUES(?, ?, ?, ?, ?, ?)"""

observer_query = """INSERT OR IGNORE INTO observers (string_id)
VALUES(?)"""

breeding_query = """INSERT OR IGNORE INTO breeding (breeding_code, breeding_category, behavior_code)
VALUES(?, ?, ?)"""

locality_query = """INSERT OR IGNORE INTO localities (locality_name, locality_code, locality_type)
VALUES(?, ?, ?)"""

protocol_query = """INSERT OR IGNORE INTO protocols (protocol_type, protocol_code, project_code)
VALUES(?, ?, ?)"""

INSERT_QUERIES = (country_query, bcr_query, iba_query, species_query, observer_query, breeding_query, locality_query, protocol_query)


observation_query = """INSERT INTO observations
(
    country_id,
    bcr_code_id,
    iba_code_id,
    species_id,
    observer_id,
    breeding_id,
    locality_id,
    protocol_id,
    global_unique_identifier,
    last_edited_date,
    observation_count,
    age_sex,
    usfws_code,
    atlas_block,
    latitude,
    longitude,
    observation_date,
    observations_started,
    sampling_event_identifier,
    duration_minutes,
    effort_distance,
    effort_area,
    number_observers,
    all_species_reported,
    group_identifier,
    has_media,
    approved,
    reviewed,
    reason,
    trip_comments,
    species_comments
)
VALUES
(
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?
);"""


def create_tables(db):
    sql = (Path(__file__).parent / 'create_tables.sql').open().read()
    db.executescript(sql)


def add_line(line: List[str], db: sqlite3.Connection, cache: Dict[str, Dict[Tuple[str, ...], int]]):
    """Unoptimized line insert

    Args:
        line (List[str]):                               The split line being inserted
        db (sqlite3.Connection):                        The database connection
        cache (Dict[str, Dict[Tuple[str, ...], int]]):  A cache of all the subtable indices. Since all the subtables are relatively small,  this is faster than actually checking the database.
    """
    ref_ids = []
    for (t_name, indices), insert_query in zip(heading_indices.items(), INSERT_QUERIES):
        values = tuple(line[x] for x in indices)
        try:
            ref_id = cache[t_name][values]
            ref_ids.append(ref_id)
        except KeyError:
            x = db.execute(insert_query, values)
            ref_ids.append(x.lastrowid)
            cache[t_name][values] = x.lastrowid
    a = [line[i] for i in range(len(line)) if i not in skip_indices]
    ref_ids.extend(a)

    x = db.execute(observation_query, ref_ids)
    return cache


def build_db(input_path: Path, output_path: Optional[Path] = None, max_lines: int=1000000, seek_to: Optional[int] = None):
    """Build a sqlite database from an input file

    Args:
        input_path: Path to the input dataset
        output_path: Desired path for the database. Defaults to {DATA_FOLDER}/{input_path.stem}.sqlite
    """
    if output_path is None:
        output_path = (config.DATA_HOME / input_path.stem).with_suffix(".sqlite")

    db = sqlite3.connect(":memory:")
    create_tables(db)

    cache = defaultdict(dict)
    with open(input_path) as input_f:
        reader = csv.reader(input_f, delimiter="\t")
        # Skip the header line
        header = tuple(x.replace(' ', '_').lower() for x in reader.__next__())
        import pdb
        pdb.set_trace()
        for i, line in enumerate(reader):
            cache = add_line(line, db, cache)

            if i > max_lines:
                # TODO: store
                break
            # db.execute(insert_query, line)
