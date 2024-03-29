# aukpy
[![CircleCI](https://circleci.com/gh/vluzko/aukpy.svg?style=shield)](https://circleci.com/gh/vluzko/aukpy)
[![Documentation Status](https://readthedocs.org/projects/aukpy/badge/?version=latest)](https://aukpy.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/vluzko/aukpy/branch/main/graph/badge.svg?token=YCS96F5R5F)](https://codecov.io/gh/vluzko/aukpy)

A reimplementation of [auk](https://github.com/CornellLabofOrnithology/auk) in Python.

Note that this is *not* a direct port: aukpy does not use `awk` to access data. Instead it provides a similar API, but uses sqlite to index and query the dataset.

For detailed usage, [see the full documentation](https://aukpy.readthedocs.io/en/latest/)

## Usage
To load and filter a set of eBird observations stored in `observations.txt`:
```
from aukpy import db, queries
db_conn = db.build_db_pandas('observations.txt')
df = queries.species('Sturnus vulgaris').run(db_conn)
```


## Performance
Observation file size vs `aukpy` file size:
[![File size](https://github.com/vluzko/aukpy/blob/compression-ratio/docs/size.png)](https://github.com/vluzko/aukpy/blob/compression-ratio/docs/size.png)
