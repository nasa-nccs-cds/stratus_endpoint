from stratus_endpoint.collections.scan import FileScanner
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional, Iterable, Union
from glob import glob
import os, time, math
import multiprocessing as mp
from multiprocessing import Pool

regen_colls = [ "merra2", "cip" ]

base_dir = "/nfs4m/css/curated01"
coll_dir = "/att/pubrepo/ILAB/data/collections/agg"

collection_specs = [
    { "cip-merra2": "create-ip/data/reanalysis/NASA-GMAO/GEOS-5/MERRA2/*" },
    { "cip-merra2-asm": "create-ip/data/reanalysis/NASA-GMAO/GEOS-5/MERRA2/*" },
    { "merra2": "/merra2/data/*" }
]

def process_collection( collection_spec: Dict ):
    for collId, file_paths in collection_spec.items():
        for collDir in glob(f"{base_dir}/{file_paths}"):
            collName = os.path.basename(collDir).lower()
            scanner2 = FileScanner( f"{collId}-{collName}", path=collDir, ext="nc" )
            scanner2.write( coll_dir )


t0 = time.time()
nproc = 2*mp.cpu_count()
with Pool(processes=nproc) as pool:
    pool.map( process_collection, collection_specs )


print( f"Completed collection generation in {(time.time()-t0)/60.0} minutes")