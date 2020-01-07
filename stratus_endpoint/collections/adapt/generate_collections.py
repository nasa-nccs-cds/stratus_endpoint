from stratus_endpoint.collections.scan import FileScanner
from glob import glob
import os

base_dir = "/nfs4m/css/curated01"

for collDir in glob(f"{base_dir}/merra2/data/*"):
    collName = os.path.basename(collDir)
    scanner2 = FileScanner( f"merra2.{collName}", path=collDir, ext="nc2" )
    scanner2.write( "/att/pubrepo/ILAB/data/collections/agg" )

