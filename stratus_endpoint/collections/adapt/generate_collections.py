from stratus_endpoint.collections.scan import FileScanner
from glob import glob
import os

regen_colls = [ "merra2", "cip" ]

base_dir = "/nfs4m/css/curated01"
coll_dir = "/att/pubrepo/ILAB/data/collections/agg"

if "cip" in regen_colls:
    for collDir in glob(f"{base_dir}/create-ip/data/reanalysis/*"):
        collName = os.path.basename(collDir).lower()
        scanner2 = FileScanner( f"cip.{collName}", path=collDir, ext="nc" )
        scanner2.write( coll_dir )

if "merra2" in regen_colls:
    for collDir in glob(f"{base_dir}/merra2/data/*"):
        collName = os.path.basename(collDir).lower()
        scanner2 = FileScanner( f"merra2.{collName}", path=collDir, ext="nc4" )
        scanner2.write( coll_dir )

