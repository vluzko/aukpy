import subprocess
from pathlib import Path
from time import time

import aukpy


def run_auk():
    auk_path = Path(__file__).parent / "auk_test.r"
    subprocess.run(
        ["Rscript", str(auk_path)],
    )


def run_aukpy():
    db = aukpy.db.build_db_incremental(Path(__file__).parent / "observations.txt")


def main():
    start = time()
    run_auk()
    end = time()
    print(end - start)
    start = time()
    run_aukpy()
    end = time()
    print(end - start)


if __name__ == "__main__":
    main()
