# Contributing
Because of the restrictions around redistribution of eBird data, any data used for testing can't be shared. The solution to this is to mock data sets that are still valid for testing purposes, but until then replicating tests is very difficult.

## Reporting Bugs
If you find a bug/issue with processing any of your eBird data, please submit it on the [issues page](https://github.com/vluzko/aukpy/issues), along with the filters used to generate the underlying dataset. This will allow me to request the same dataset from eBird myself and replicate the error.

## Building Documentation
Once sphinx is installed, simply run:
- `cd docs/`
- `sphinx-build . _build/`
