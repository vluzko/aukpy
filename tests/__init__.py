from pathlib import Path

TEST_DATA = Path(__file__).parent / "data"
SMALL = TEST_DATA / "small" / "observations.txt"
MEDIUM = TEST_DATA / "medium" / "observations.txt"
LARGE = TEST_DATA / "large" / "observations.txt"
# A clean dataframe with non empty atlas codes
WITH_ATLAS = TEST_DATA / "with_atlas.csv"

SMALL_DB = TEST_DATA / "small" / "observations.sqlite"
MEDIUM_DB = TEST_DATA / "medium" / "observations.sqlite"
LARGE_DB = TEST_DATA / "large" / "observations.sqlite"
