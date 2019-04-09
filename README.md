# stratus_endpoint
Base classes for creating service endpoints for the [Stratus Framework](https://github.com/nasa-nccs-cds/stratus)

These classes wrap python services for integration with other services using Stratus.

### Conda environment setup

```
 >> conda create -n stratus -c conda-forge python=3.6 libnetcdf netCDF4 pyyaml 
 ```

### Installation

```
 >> python setup.py install
```

##### Developer Notes: Uploading to PyPi

```
   >> python setup.py sdist bdist_wheel
   >> python -m twine upload --repository-url https://upload.pypi.org/legacy/ ./dist/*
```
