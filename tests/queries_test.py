import pandas as pd
import pytest
import sqlite3
from aukpy import queries

from tests import SMALL_DB, MEDIUM_DB, SKIP_NON_MOCKED


@pytest.mark.skipif(**SKIP_NON_MOCKED)  # type: ignore
def test_species_filter():
    conn = sqlite3.connect(str(SMALL_DB))
    f = queries.species("House Sparrow")
    result = f.run_pandas(conn)
    assert len(result) == 192

    assert (result["scientific_name"] == "Passer domesticus").all()


@pytest.mark.skipif(**SKIP_NON_MOCKED)  # type: ignore
def test_date_filter():
    conn = sqlite3.connect(str(MEDIUM_DB))
    f = queries.date(after="2015-01-01", before="2015-01-05")
    res = f.run_pandas(conn)
    # This probably shouldn't be hard coded, given data sharing restrictions
    assert len(res) == 38018


@pytest.mark.skipif(**SKIP_NON_MOCKED)  # type: ignore
def test_duration_filter():
    conn = sqlite3.connect(str(MEDIUM_DB))
    res = queries.duration(maximum=5).run_pandas(conn)
    assert len(res) == 18005


@pytest.mark.skipif(**SKIP_NON_MOCKED)  # type: ignore
def test_time_filter():
    conn = sqlite3.connect(str(MEDIUM_DB))
    res = queries.time(after="03:00", before="06:00").run_pandas(conn)
    assert len(res) == 17056


@pytest.mark.skipif(**SKIP_NON_MOCKED)  # type: ignore
def test_country_filter():
    conn = sqlite3.connect(str(SMALL_DB))
    res = queries.country(("US", "Mexico")).run_pandas(conn)
    assert len(res) == 10000


@pytest.mark.skipif(**SKIP_NON_MOCKED)  # type: ignore
def test_distance_filter():
    conn = sqlite3.connect(str(MEDIUM_DB))
    res = queries.distance(0.0, 0.2).run_pandas(conn)
    assert "Stationary" in res["protocol_type"].unique()
    assert (res["effort_distance_km"] <= 0.2).all()

    res = queries.distance(0.1, 0.2).run_pandas(conn)
    assert (res["effort_distance_km"] >= 0.1).all()


@pytest.mark.skipif(**SKIP_NON_MOCKED)  # type: ignore
def test_breeding_filter():
    conn = sqlite3.connect(str(MEDIUM_DB))
    res = queries.breeding("CF").run_pandas(conn)
    assert (res["breeding_code"] == "CF").all()
    assert len(res) == 54

    res = queries.breeding(("CF", "CN")).run_pandas(conn)
    assert set(res["breeding_code"]) == {"CF", "CN"}
    assert len(res["breeding_code"]) == 217


@pytest.mark.skipif(**SKIP_NON_MOCKED)  # type: ignore
def test_project_filter():
    conn = sqlite3.connect(str(MEDIUM_DB))
    res = queries.project("EBIRD_CAN").run_pandas(conn)
    assert (res["project_code"] == "EBIRD_CAN").all()
    assert len(res) == 1291

    res = queries.project(("EBIRD_CAN", "EBIRD_QC")).run_pandas(conn)
    assert set(res["project_code"]) == {"EBIRD_CAN", "EBIRD_QC"}
    assert len(res["project_code"]) == 1313


@pytest.mark.skipif(**SKIP_NON_MOCKED)  # type: ignore
def test_protocol_filter():
    conn = sqlite3.connect(str(MEDIUM_DB))
    res = queries.protocol("Stationary").run_pandas(conn)
    assert (res["protocol_type"] == "Stationary").all()
    assert len(res) == 307485

    res = queries.protocol(("Stationary", "Incidental")).run_pandas(conn)
    assert set(res["protocol_type"]) == {"Stationary", "Incidental"}
    assert len(res["protocol_type"]) == 359921


@pytest.mark.skipif(**SKIP_NON_MOCKED)  # type: ignore
def test_last_edited_filter():
    conn = sqlite3.connect(str(MEDIUM_DB))
    res = queries.last_edited("2022-01-01").run_pandas(conn)

    assert res["last_edited_date"].min() > pd.to_datetime("2022-01-01").timestamp()
    assert len(res) == 27116


@pytest.mark.skipif(**SKIP_NON_MOCKED)  # type: ignore
def test_wildcard_filter():
    conn = sqlite3.connect(str(MEDIUM_DB))
    res = queries.date("*-02-05", "*-02-07").run_pandas(conn)
    assert len(res) == 11915
