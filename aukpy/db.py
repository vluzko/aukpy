"""Tools for building a sqlite database containing the eBird dataset.
The full dataset is distributed as a ~400GB csv file, much of which is redundant.
We store it in a fully normalized sqlite file instead, which reduces the size of the data
and makes querying it much faster.
"""
import pandas as pd
import pickle
import sqlite3

from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

from torch import not_equal

from aukpy import config


HEADINGS = tuple(
    x.replace(" ", "_").lower()
    for x in (
        "GLOBAL UNIQUE IDENTIFIER",
        "LAST EDITED DATE",
        "TAXONOMIC ORDER",
        "CATEGORY",
        "TAXON CONCEPT ID",
        "COMMON NAME",
        "SCIENTIFIC NAME",
        "SUBSPECIES COMMON NAME",
        "SUBSPECIES SCIENTIFIC NAME",
        "EXOTIC CODE",
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
DF_COLUMNS = (
    "global_unique_identifier",
    "last_edited_date",
    "observation_count",
    "age_sex",
    "usfws_code",
    "atlas_block",
    "latitude",
    "longitude",
    "observation_date",
    "time_observations_started",
    "sampling_event_identifier",
    "duration_minutes",
    "effort_distance_km",
    "effort_area_ha",
    "number_observers",
    "all_species_reported",
    "group_identifier",
    "has_media",
    "approved",
    "reviewed",
    "reason",
    "trip_comments",
    "species_comments",
    "exotic_code",
    "taxonomic_order",
    "category",
    "common_name",
    "scientific_name",
    "subspecies_common_name",
    "subspecies_scientific_name",
    "taxon_concept_id",
    "country",
    "country_code",
    "state",
    "state_code",
    "county",
    "county_code",
    "locality",
    "locality_id",
    "locality_type",
    "bcr_code",
    "iba_code",
    "breeding_code",
    "breeding_category",
    "behavior_code",
    "protocol_type",
    "protocol_code",
    "project_code",
    "observer_id",
)


observer_query = """INSERT OR IGNORE INTO observer (string_id)
VALUES(?)"""


def create_tables(db):
    sql = (Path(__file__).parent / "create_tables.sql").open().read()
    db.executescript(sql)


def undo_compression(df: pd.DataFrame) -> pd.DataFrame:
    """Undo the data compression performed when storing the dataframe in sqlite.
    Mostly this is just converting things back into strings
    """
    g_prefix = "URN:CornellLabOfOrnithology:EBIRD:OBS"
    df["global_unique_identifier"] = g_prefix + df["global_unique_identifier"].astype(
        str
    )

    df["observation_count"] = df["observation_count"].astype(str)

    df["sampling_event_identifier"] = "S" + df["sampling_event_identifier"].astype(str)

    df["locality_id"] = "L" + df["locality_id"].astype(str)

    df["observer_id"] = "obsr" + df["observer_id"].astype(str)

    empty = df["usfws_code"].isna()
    df.loc[~empty, "usfws_code"] = "USFWS_" + df[~empty]["usfws_code"].astype(
        int
    ).astype(str)

    df["observation_date"] = pd.to_datetime(df["observation_date"] * 1e9).astype(str)

    empty_time = df["time_observations_started"].isna()
    s = pd.to_datetime(
        df.loc[~empty_time, "time_observations_started"], unit="s"
    ).dt.time.astype(str)
    df.loc[~empty_time, "time_observations_started"] = s

    not_empty_gi = ~df["group_identifier"].isna()
    s = "G" + df[not_empty_gi]["group_identifier"].astype(float).astype(int).astype(str)
    df.loc[not_empty_gi, "group_identifier"] = s

    return df


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
        # Table specific preprocessing
        sub_frame = cls.df_processing(df.loc[:, cls.columns])
        max_id = db.execute("SELECT MAX(id) FROM {}".format(cls.table_name)).fetchone()[
            0
        ]
        max_id = max_id if max_id is not None else 0
        # TODO: Optimization: Sort and drop_duplicates is probably faster.
        unique_values = sub_frame.groupby(sub_frame.columns.tolist()).groups

        # Table specific values processing (needed for IBA, BCR)
        values = cls.values_processing([x for x in unique_values.keys()])
        db.executemany(cls.insert_query, values)
        group_ids = {
            k: v
            for k, v in zip(
                unique_values.keys(), range(max_id + 1, max_id + len(values) + 1)
            )
        }
        idx_id_map = {
            idx: group_ids[k] for k, indices in unique_values.items() for idx in indices
        }
        as_series = pd.Series(idx_id_map)
        df[f"{cls.table_name}_id"] = as_series
        return df


class LocationWrapper(TableWrapper):
    table_name = "location_data"
    columns = (
        "country",
        "country_code",
        "state",
        "state_code",
        "county",
        "county_code",
        "locality",
        "locality_id",
        "locality_type",
        "usfws_code",
        "atlas_block",
        "bcr_code",
        "iba_code",
    )
    insert_query = """INSERT OR IGNORE INTO location_data
    ('country', 'country_code', 'state', 'state_code', 'county', 'county_code', 'locality', 'locality_id', 'locality_type', 'usfws_code', 'atlas_block', 'bcr_code', 'iba_code')
    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

    @classmethod
    def df_processing(cls, df: pd.DataFrame) -> pd.DataFrame:
        s = df["locality_id"].str[1:].astype(int)
        df["locality_id"] = s

        empty = df["usfws_code"].isna()
        df.loc[~empty, "usfws_code"] = (
            df[~empty]["usfws_code"].astype(str).str[6:].astype(float)
        )

        return df


class SpeciesWrapper(TableWrapper):
    table_name = "species"
    columns = (
        "taxonomic_order",
        "category",
        "common_name",
        "scientific_name",
        "subspecies_common_name",
        "subspecies_scientific_name",
        "taxon_concept_id",
    )
    insert_query = """INSERT OR IGNORE INTO species
    (taxonomic_order, category, common_name, scientific_name, subspecies_common_name, subspecies_scientific_name, taxon_concept_id)
    VALUES(?, ?, ?, ?, ?, ?, ?)"""


class BreedingWrapper(TableWrapper):
    table_name = "breeding"
    columns = ("breeding_code", "breeding_category", "behavior_code")
    insert_query = """INSERT OR IGNORE INTO breeding
    (breeding_code, breeding_category, behavior_code)
    VALUES(?, ?, ?)"""


class ProtocolWrapper(TableWrapper):
    table_name = "protocol"
    columns = ("protocol_type", "protocol_code", "project_code")
    insert_query = """INSERT OR IGNORE INTO protocol (protocol_type, protocol_code, project_code)
    VALUES(?, ?, ?)"""


class SamplingWrapper(TableWrapper):
    table_name = "sampling_event"
    columns = (
        "sampling_event_identifier",
        "observation_date",
        "time_observations_started",
        "observer_id",
        "effort_distance_km",
        "effort_area_ha",
        "duration_minutes",
        "trip_comments",
        "latitude",
        "longitude",
        "all_species_reported",
        "number_observers",
    )
    insert_query = """INSERT OR IGNORE INTO sampling_event
        (sampling_event_identifier, observation_date, time_observations_started, observer_id, effort_distance_km, effort_area_ha, duration_minutes, trip_comments, latitude, longitude, all_species_reported, number_observers)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    @classmethod
    def df_processing(cls, df: pd.DataFrame) -> pd.DataFrame:
        s = df["sampling_event_identifier"].str[1:].astype(int)
        df["sampling_event_identifier"] = s

        df["observer_id"] = df["observer_id"].str[4:].astype(int)

        # Convert to integer and then convert from nanoseconds to seconds
        df["observation_date"] = pd.to_datetime(df["observation_date"]).astype(int) // (
            10**9
        )

        # Convert time to integer
        has_time = ~df["time_observations_started"].isna()
        # split = df[has_time]["time_observations_started"].str.split(":")
        as_dt = pd.to_datetime(df[has_time]["time_observations_started"])
        seconds = (
            as_dt.dt.hour * 3600 + as_dt.dt.minute * 60 + as_dt.dt.second
        ).astype(int)
        # seconds = split.apply(lambda x: int(x[0]) * 3600 + int(x[1]) * 60 + int(x[2]))
        df.loc[has_time, "time_observations_started"] = seconds

        return df


class ObservationWrapper(TableWrapper):
    table_name = "observation"
    columns = (
        "location_data_id",
        "species_id",
        "breeding_id",
        "protocol_id",
        "sampling_event_id",
        "global_unique_identifier",
        "last_edited_date",
        "observation_count",
        "age_sex",
        "group_identifier",
        "has_media",
        "approved",
        "reviewed",
        "reason",
        "species_comments",
        "exotic_code",
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
        ?
    );""".format(
        ",\n".join(columns)
    )

    @classmethod
    def df_processing(cls, df: pd.DataFrame) -> pd.DataFrame:

        # Global unique identifier
        s = df["global_unique_identifier"].str[37:].astype(int)
        df["global_unique_identifier"] = s

        not_empty = ~df["group_identifier"].isna()
        s = df[not_empty]["group_identifier"].str[1:].astype(int)
        df.loc[not_empty, "group_identifier"] = s

        return df


WRAPPERS = (
    LocationWrapper,
    SpeciesWrapper,
    BreedingWrapper,
    ProtocolWrapper,
    SamplingWrapper,
)


def read_clean(input_path: Path) -> pd.DataFrame:
    df = pd.read_csv(input_path, sep="\t")
    renames = {x: x.lower().replace(" ", "_").replace("/", "_") for x in df.columns}
    df.rename(columns=renames, inplace=True)

    # Drop any extra columns
    for col in df.columns:
        if col not in HEADINGS:
            df.drop(col, axis=1, inplace=True)
    return df


def build_db_pandas(
    input_path: Path, output_path: Optional[Path] = None
) -> sqlite3.Connection:
    """Build a sqlite database using pandas to parse the CSV

    Args:
        input_path (Path):                      Path to the CSV of observations
        output_path (Optional[Path], optional): Location to store the database. DB will be built in memory if None Defaults to None.

    Returns:
        sqlite3.Connection: A connection to the finished database.
    """
    if output_path is None:
        conn = sqlite3.connect(":memory:")
    else:
        conn = sqlite3.connect(str(output_path.absolute()))
    create_tables(conn)
    # TODO: Max lines and seek
    df = read_clean(input_path)

    # Store subtables
    for wrapper in WRAPPERS:
        df = wrapper.insert(df, conn)

    # Store main observations table
    used_columns = [y for x in WRAPPERS for y in x.columns]
    just_obs = df.drop(used_columns, axis=1)
    ObservationWrapper.insert(just_obs, conn)
    conn.commit()
    return conn


def build_db_incremental(
    input_path: Path, output_path: Path, max_size: int = 1000000
) -> sqlite3.Connection:
    """Build a database incrementally.
    Useful for very large observation files (>10GB).

    Args:
        input_path (Path):                      Path to the CSV of observations.
        output_path (Path):                     Location to store the database.
        max_lines (int, optional):              The maximum number of bytes of the CSV to read at a time.
    """
    cache_meta_path = config.DATA_HOME / f"{output_path.stem}_meta.pkl"

    if cache_meta_path.is_file():
        cache_meta = pickle.load(cache_meta_path.open("rb"))
    else:
        cache_meta = {"seek_to": 0}

    conn = sqlite3.connect(str(output_path.absolute()))
    # Check for tables

    # Load subtables

    raise NotImplementedError
