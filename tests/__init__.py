from pathlib import Path


TEST_DATA = Path(__file__).parent / "data"
MOCK_DATA = Path(__file__).parent / "mocked"
SMALL = TEST_DATA / "small" / "observations.txt"
MEDIUM = TEST_DATA / "medium" / "observations.txt"
LARGE = TEST_DATA / "large" / "observations.txt"
SUBSAMPLED_DIR = TEST_DATA / "subsampled"

# We skip any unmocked tests if the file doesn't exist
SKIP_NON_MOCKED = {
    "condition": not SMALL.exists() or not MEDIUM.exists(),
    "reason": "Real datasets not available",
}

SMALL_DB = TEST_DATA / "small" / "observations.sqlite"
MEDIUM_DB = TEST_DATA / "medium" / "observations.sqlite"
LARGE_DB = TEST_DATA / "large" / "observations.sqlite"

M_SMALL = MOCK_DATA / "small.txt"
M_MEDIUM = MOCK_DATA / "medium.txt"
M_SMALL1 = MOCK_DATA / "small1.txt"
M_SMALL2 = MOCK_DATA / "small2.txt"
M_SMALL3 = MOCK_DATA / "small3.txt"

SMALL_MOCKED = (M_SMALL1, M_SMALL2, M_SMALL3)


def generate_mock_data(obs_path: Path, out_path: Path):
    """Convert the real medium sized dataset to a fake dataset"""
    from aukpy import db
    from tests import data_utils

    df = db.read_clean(obs_path)
    new_df = data_utils.scramble_observations(df)
    new_df.to_csv(out_path, index=False, sep="\t")


def generate_subsampled(obs_path: Path, out_path: Path, num_rows: int = 10000):
    from aukpy import db
    from tests import data_utils

    df = db.read_clean(obs_path)
    subsampled = data_utils.subsample(df, num_rows=num_rows)
    subsampled.to_csv(out_path, index=False, sep="\t")


# generate_mock_data(SMALL, M_SMALL)
# generate_mock_data(MEDIUM, M_MEDIUM)
# generate_subsampled(M_MEDIUM, M_SMALL1)
# generate_subsampled(M_MEDIUM, M_SMALL2)
# generate_subsampled(M_MEDIUM, M_SMALL3)
