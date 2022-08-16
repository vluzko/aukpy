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
    # Scramble locations
    just_first = df.drop_duplicates("sampling_event_identifier").set_index(
        "sampling_event_identifier"
    )
    location_shuffle = np.random.permutation(just_first.index)
    just_loc = just_first.loc[location_shuffle, list(db.LocationWrapper.columns)]

    merged = just_loc.join(s_id_to_index).reset_index().set_index("index").loc[df.index]
    assert len(merged) == len(df)
    df[list(db.LocationWrapper.columns)] = merged[list(db.LocationWrapper.columns)]

    # for col in db.LocationWrapper.columns:
    #     df[col] = df.loc[location_shuffle, col]
    # Scramble sampling events

    # Scramble species

    # Generate random trip comments

    # Generate random species comments

    # Generate random observer ids
    import pdb

    pdb.set_trace()
