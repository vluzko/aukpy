from typing import List
import numpy as np
import pandas as pd
from aukpy import db


def scramble_observations(df: pd.DataFrame) -> pd.DataFrame:
    """Scramble and anonymize a real set of observations"""

    # sampling_events = df.groupby('sampling_event_identifier')
    s_id_to_index = (
        df[["sampling_event_identifier"]]
        .reset_index()
        .set_index("sampling_event_identifier")
    )
    # Shuffle locations
    just_first = df.drop_duplicates("sampling_event_identifier").set_index(
        "sampling_event_identifier"
    )
    location_shuffle = np.random.permutation(just_first.index)
    just_loc = just_first.loc[location_shuffle, list(db.LocationWrapper.columns)]

    merged = just_loc.join(s_id_to_index).reset_index().set_index("index").loc[df.index]
    assert len(merged) == len(df)
    df[list(db.LocationWrapper.columns)] = merged[list(db.LocationWrapper.columns)]

    # Shuffle sampling events
    to_scramble = list(db.SamplingWrapper.columns)
    del to_scramble[0]

    obs_shuffle = np.random.permutation(just_first.index)
    just_obs = just_first.loc[obs_shuffle, list(to_scramble)]

    # Randomize observer ids
    just_obs["observer_id"] = just_obs["observer_id"].str[4:].astype(int)
    r = just_obs["observer_id"]
    observers = r.unique()
    new_ids = np.random.randint(r.min(), r.max(), len(observers))
    new_observers = {o: n for o, n in zip(observers, new_ids)}
    just_obs["observer_id"] = "obsr" + r.map(new_observers).astype(str)

    merged = just_obs.join(s_id_to_index).reset_index().set_index("index").loc[df.index]
    assert len(merged) == len(df)
    df[to_scramble] = merged[to_scramble]

    # Scramble species
    # Obviously this does not preserve the geographic distribution of species
    species_shuffle = np.random.permutation(df.index)

    for col in db.SpeciesWrapper.columns:
        df[col] = df.loc[species_shuffle, col].values

    # Remove comments
    for col in ("trip_comments", "species_comments"):
        not_empty = ~df[col].isna()
        df.loc[not_empty, col] = ""

    return df


def subsample(df: pd.DataFrame, num_rows: int = 100000) -> pd.DataFrame:
    """Extract a random set of rows from the dataframe"""
    assert len(df) >= num_rows
    shuffle = np.random.permutation(df.index)
    return df.loc[shuffle].iloc[:num_rows]
