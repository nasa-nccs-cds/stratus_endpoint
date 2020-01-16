import os, xarray, time, math
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
from stratus_endpoint.util.fileTest import FileTester

base_dirs = "/css/merra2/data/*"
suffix = ".nc4"

fileTester = FileTester( "merra2", suffix )

fileTester.search( base_dirs )