from stratus_endpoint.collections.scan import FileScanner
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional, Iterable, Union
from glob import glob
import os, time, math

regen_colls = [ "merra2", "cip" ]

base_dir = "/nfs4m/css/curated01"
coll_dir = "/att/pubrepo/ILAB/data/collections/agg"

collection_specs0 = [
    { "merra2": "/merra2/data/*" }
]

collection_specs1 = [
    { "cip_merra2": "create-ip/data/reanalysis/NASA-GMAO/GEOS-5/MERRA2/*" },
    { "cip_merra2_asm": "create-ip/data/reanalysis/NASA-GMAO/GEOS-5/MERRA2/*" },
    { "noaa_ncep_cfsr": "create-ip/data/reanalysis/NOAA-NCEP/CFSR/*" },
    { "noaa_ncep_mom3": "create-ip/data/reanalysis/NOAA-NCEP/MOM3/*" },
    { "noaa_gfdl_mom4": "create-ip/data/reanalysis/NOAA-GFDL/MOM4/ECDAv31/*"},
    { "noaa_esrl+cires": "create-ip/data/reanalysis/NOAA-ESRLandCIRES/ensda-v351/20CRv2c/*"},
    { "ecmwf_ifs_erai": "create-ip/data/reanalysis/ECMWF/IFS-Cy31r2/ERA-Interim/*"},
    { "ecmwf_ifs_era5": "create-ip/data/reanalysis/ECMWF/IFS-Cy41r2/ERA5/*"},
    { "ecmwf_ifs_cera": "create-ip/data/reanalysis/ECMWF/IFS-Cy41r2/CERA-20C/*"},
    { "ecmwf_nemo_oras4":    "create-ip/data/reanalysis/ECMWF/NEMOv3/ORAS4/*"},
    { "ecmwf_nemo_orap5":    "create-ip/data/reanalysis/ECMWF/NEMOv34+LIM2/ORAP5/*"},
    { "jma_jra35":      "create-ip/data/reanalysis/JMA/JRA-25/*"},
    { "jma_jra55":      "create-ip/data/reanalysis/JMA/JRA-55/*"},
    { "jma_jra55_mdl_iso": "create-ip/data/reanalysis/JMA/JRA-55-mdl-iso/*"},
    { "cmcc_nemo+lim2": "create-ip/data/reanalysis/CMCC/NEMOv32+LIM2/C-GLORSv5/*"},
    { "iap-ua_ccsm-cam_era40": "create-ip/data/reanalysis/IAP-UA/CCSM-CAM/ERA40-CRUTS3-10/*"},
    { "iap-ua_ccsm_era40": "create-ip/data/reanalysis/IAP-UA/CCSM-CAM/ERA40-CRUTS3-10/*"},
    { "iap-ua_ncep-gom_cruts3": "create-ip/data/reanalysis/IAP-UA/NCEP-Global-Operational-Model/NCEP-NCAR-CRUTS3-10/*"},
    { "uh-mitgcm_gecco2": "create-ip/data/reanalysis/University-Hamburg/MITgcm/GECCO2/*"}
]



def process_collection( collection_spec: Dict ):
    for collId, file_paths in collection_spec.items():
        for collDir in glob(f"{base_dir}/{file_paths}"):
            collName = os.path.basename(collDir).lower()
            scanner2 = FileScanner( f"{collId}_{collName}", path=collDir, ext=[".nc4",".nc"] )
            scanner2.write( coll_dir )

t0 = time.time()

for collection_spec in collection_specs0:
    process_collection(collection_spec)

print( f"Completed collection generation in {(time.time()-t0)/60.0} minutes")