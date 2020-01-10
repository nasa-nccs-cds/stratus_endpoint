import os, xarray, time, math
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
from stratus_endpoint.util.fileTest import FileTester

base_dirs = "/nfs4m/css/curated01/create-ip/data/*"
suffix = ".nc"

fileTester = FileTester( "cip", suffix, clean=True )

fileTester.search( base_dirs )



