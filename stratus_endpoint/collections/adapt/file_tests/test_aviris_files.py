import os, xarray, time, math
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
from collection_intake.util.fileTest import FileTester

base_dirs = "/att/pubrepo/ABoVE/archived_data/ORNL/ABoVE_Airborne_AVIRIS_NG/data/*"
suffix = "_img"
engine = "rasterio"

fileTester = FileTester( "aviris", suffix, engine=engine, clean=True )

fileTester.search( base_dirs )



