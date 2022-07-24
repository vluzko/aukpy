"""Tools for building a sqlite database containing the eBird dataset.
The full dataset is distributed as a ~400GB csv file, much of which is redundant.
We store it in a fully normalized sqlite file instead, which reduces the size of the data
and makes querying it much faster.
"""
import numpy as np
import pandas as pd
import sqlite3
import csv

from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

from aukpy import config


HEADINGS = tuple(
    x.replace(" ", "_").lower()
    for x in (
        "GLOBAL UNIQUE IDENTIFIER",
        "LAST EDITED DATE",
        "TAXONOMIC ORDER",
        "CATEGORY",
        'TAXON CONCEPT ID',
        "COMMON NAME",
        "SCIENTIFIC NAME",
        "SUBSPECIES COMMON NAME",
        "SUBSPECIES SCIENTIFIC NAME",
        'EXOTIC CODE',
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

# The columns present when we load the data into a dataframe
DF_COLUMNS = ( 'global_unique_identifier',
    'last_edited_date', 'observation_count', 'age_sex', 'usfws_code',
    'atlas_block', 'latitude', 'longitude', 'observation_date',
    'time_observations_started', 'sampling_event_identifier',
    'duration_minutes', 'effort_distance_km', 'effort_area_ha',
    'number_observers', 'all_species_reported', 'group_identifier',
    'has_media', 'approved', 'reviewed', 'reason', 'trip_comments',
    'species_comments', 'exotic_code', 'taxonomic_order', 'category',
    'common_name', 'scientific_name', 'subspecies_common_name',
    'subspecies_scientific_name', 'taxon_concept_id', 'country_name',
    'country_code', 'state_name', 'state_code', 'county_name',
    'county_code', 'locality', 'locality_id', 'locality_type',
    'bcr_code', 'iba_code', 'breeding_code',
    'breeding_category', 'behavior_code', 'protocol_type',
    'protocol_code', 'project_code'
)

# We could construct these, but since there's only a few it's better to just hardcode them
location_query = """INSERT OR IGNORE INTO location_data ('country_name', 'country_code', 'state_name', 'state_code', 'county_name', 'county_code', 'locality', 'locality_id', 'locality_type')
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)"""

bcr_query = """INSERT OR IGNORE INTO bcrcode (bcr_code)
VALUES(?)"""

iba_query = """INSERT OR IGNORE INTO ibacode (iba_code)
VALUES(?)"""

species_query = """INSERT OR IGNORE INTO species (taxonomic_order, category, common_name, scientific_name, subspecies_common_name, subspecies_scientific_name, taxon_concept_id)
VALUES(?, ?, ?, ?, ?, ?, ?)"""

observer_query = """INSERT OR IGNORE INTO observer (string_id)
VALUES(?)"""

breeding_query = """INSERT OR IGNORE INTO breeding (breeding_code, breeding_category, behavior_code)
VALUES(?, ?, ?)"""

protocol_query = """INSERT OR IGNORE INTO protocol (protocol_type, protocol_code, project_code)
VALUES(?, ?, ?)"""


def create_tables(db):
    sql = (Path(__file__).parent / 'create_tables.sql').open().read()
    db.executescript(sql)


class TableWrapper:
    table_name: str
    columns: Tuple[str, ...]
    insert_query: str

    @classmethod
    def df_processing(cls, df: pd.DataFrame) -> pd.DataFrame:
        return df

    @classmethod
    def values_processing(cls, values: List[Any]) -> List[Any]:
        return values

    @classmethod
    def insert(cls, df: pd.DataFrame, db: sqlite3.Connection) -> pd.DataFrame:
        print(f'\nStoring {cls.table_name}')
        # Table specific preprocessing
        sub_frame = cls.df_processing(df.loc[:, cls.columns])
        max_id = db.execute('SELECT MAX(id) FROM {}'.format(cls.table_name)).fetchone()[0]
        max_id = max_id if max_id is not None else 0
        # TODO: Optimization: Sort and drop_duplicates is probably faster.
        unique_values = sub_frame.groupby(sub_frame.columns.tolist()).groups

        # Table specific values processing (needed for IBA, BCR)
        values = cls.values_processing([x for x in unique_values.keys()])
        db.executemany(cls.insert_query, values)
        group_ids = {k: v for k, v in zip(unique_values.keys(), range(max_id + 1, max_id + len(values) + 1))}
        idx_id_map = {idx: group_ids[k] for k, indices in unique_values.items() for idx in indices}
        as_series = pd.Series(idx_id_map)
        df[f'{cls.table_name}_id'] = as_series
        return df


class LocationWrapper(TableWrapper):
    table_name = 'location_data'
    columns = ('country', 'country_code', 'state', 'state_code', 'county', 'county_code', 'locality', 'locality_id', 'locality_type')
    insert_query = location_query


class BCRCodes(TableWrapper):
    table_name = 'bcrcode'
    columns = ('bcr_code', )
    insert_query = bcr_query

    @classmethod
    def values_processing(cls, values: List[Any]) -> List[Tuple[int]]:
        return [(int(x), ) for x in values]


class IBACodes(TableWrapper):
    table_name = 'ibacode'
    columns = ('iba_code', )
    insert_query = iba_query

    @classmethod
    def values_processing(cls, values: List[str]) -> List[Tuple[str]]:
        return [(x, ) for x in values]


class SpeciesWrapper(TableWrapper):
    table_name = 'species'
    columns = ('taxonomic_order', 'category', 'common_name', 'scientific_name', 'subspecies_common_name', 'subspecies_scientific_name', 'taxon_concept_id')
    insert_query = species_query


class BreedingWrapper(TableWrapper):
    table_name = 'breeding'
    columns = ('breeding_code', 'breeding_category', 'behavior_code')
    insert_query = breeding_query


class ProtocolWrapper(TableWrapper):
    table_name = 'protocol'
    columns = ('protocol_type', 'protocol_code', 'project_code')
    insert_query = protocol_query


class ObservationWrapper(TableWrapper):
    table_name = 'observation'
    columns = (
        'location_data_id',
        'bcrcode_id',
        'ibacode_id',
        'species_id',
        'observer_id',
        'breeding_id',
        'protocol_id',
        'global_unique_identifier',
        'last_edited_date',
        'observation_count',
        'age_sex',
        'usfws_code',
        'atlas_block',
        'latitude',
        'longitude',
        'observation_date',
        'time_observations_started',
        'sampling_event_identifier',
        'duration_minutes',
        'effort_distance_km',
        'effort_area_ha',
        'number_observers',
        'all_species_reported',
        'group_identifier',
        'has_media',
        'approved',
        'reviewed',
        'reason',
        'trip_comments',
        'species_comments',
        'exotic_code'
    )

    insert_query = """INSERT INTO observation
    (
        {}
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
    );""".format(',\n'.join(columns))
WRAPPERS = (LocationWrapper, BCRCodes, IBACodes, SpeciesWrapper, BreedingWrapper, ProtocolWrapper)


def build_db_pandas(input_path: Path, output_path: Optional[Path] = None, max_lines: int=1000000, seek_to: Optional[int] = None) -> sqlite3.Connection:
    """Build a sqlite database using pandas to parse the CSV

    Args:
        input_path (Path):                      Path to the CSV of observations
        output_path (Optional[Path], optional): Location to store the database. DB will be built in memory if None Defaults to None.
        max_lines (int, optional):              The maximum number of lines of the CSV to read. Defaults to 1000000.
        seek_to (Optional[int], optional):      The position in the CSV to start reading lines at. Defaults to None.

    Returns:
        sqlite3.Connection: A connection to the finished database.
    """
    if output_path is None:
        db = sqlite3.connect(':memory:')
    else:
        db = sqlite3.connect(str(output_path.absolute()))
    create_tables(db)
    # TODO: Max lines and seek
    df = pd.read_csv(input_path, sep="\t")
    renames = {x: x.lower().replace(' ', '_').replace('/', '_') for x in df.columns}
    df.rename(columns=renames, inplace=True)

    # Drop any extra columns
    for col in df.columns:
        if col not in HEADINGS:
            df.drop(col, axis=1, inplace=True)

    # Store subtables
    for wrapper in WRAPPERS:
        df = wrapper.insert(df, db)

    # Store main observations table
    used_columns = [y for x in WRAPPERS for y in x.columns]
    just_obs = df.drop(used_columns, axis=1)
    ObservationWrapper.insert(just_obs, db)
    db.commit()
    return db