# stratus_endpoint
Base classes for creating service endpoints for the [Stratus Framework](https://github.com/nasa-nccs-cds/stratus)

These classes wrap python services for integration with other services using Stratus.

### Conda environment setup

```
 >> conda create -n stratus -c conda-forge python=3.6 libnetcdf netCDF4 
 ```

### Installation

```
 >> python setup.py install
```

### Collections

  Use the *cscan* script in the *bin* directory to create collections:

```
usage: cscan [-h] [-path PATH] [-ext EXT] [-globs GLOBS] [-glob GLOB] [-mp MP]
             collectionName

Scan the file system to create a collection

positional arguments:
  collectionName  A name for the collection

optional arguments:
  -h, --help      show this help message and exit
  -path PATH      The top level (input) data directory containing all files for the collection
  -cpath CPATH    The (output) collections directory, containing the generated collection definitions, defaults to $HPDA_COLLECTIONS_DIR
  -ext EXT        The file extension for all files in the collection (used only with '-path', default: nc)
  -globs GLOBS    A comma-separated list of unix file system globs for selecting files in the collection
  -glob GLOB      A single unix file system glob for selecting files in the collection
  -mp MP          Use multiprocessing (true/false), default: true
```

##### Developer Notes: Uploading to PyPi

```
   >> python setup.py sdist bdist_wheel
   >> python -m twine upload --repository-url https://upload.pypi.org/legacy/ ./dist/*
```
