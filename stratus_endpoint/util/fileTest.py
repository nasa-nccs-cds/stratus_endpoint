import os, xarray, time, math
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional, Union
from multiprocessing import Pool
import multiprocessing as mp
from glob import glob

class FileTester:

    def __init__(self, name: str, suffix: str, **kwargs ):
        self._suffix = suffix
        self._engine = kwargs.get( 'engine' )
        self._name = name
        self._verbose = kwargs.get( 'verbose', False)
        self._errors_file = f"/tmp/bad_files-{name}.csv"
        clean = kwargs.get( 'clean', False )
        if clean:
            try: os.remove( self._errors_file )
            except: pass

    def search(self, base_path_globs: Union[str,List[str]]):
        t0 = time.time()
        if isinstance(base_path_globs, list):
            base_paths = sum([ glob(path_glob, recursive=True) for path_glob in base_path_globs], [])
        else:
            base_paths = glob( base_path_globs )

        nproc = 2*mp.cpu_count()
        with Pool(processes=nproc) as pool:
            error_lists = pool.map( self._test_files, base_paths )

        print( f"Completed test_files in {(time.time()-t0)/60.0} minutes using {nproc} processes"   )

        bad_files = open(self._errors_file, "w")
        error_count = 0
        for error_list in error_lists:
            bad_files.writelines( error_list )
            error_count = error_count + len( error_list )
        bad_files.close()
        print( f"Wrote bad files list to {self._errors_file}, nfiles: {error_count}" )

    def _test_files( self, base_dir: str ):
        errors = []
        if not base_dir.endswith(".gz"):
            good_files = 0
            for root, dirs, files in os.walk( base_dir ):
                if len(files) > 0:
                   for fname in files:
                       if fname.endswith( self._suffix ):
                           file_path = os.path.join( root, fname )
                           try:
                               if self._engine == "rasterio":
                                   ds = xarray.open_rasterio( file_path )
                               elif self._engine:
                                   ds = xarray.open_dataset( file_path, engine=self._engine, decode_times=False, decode_coords=False )
                               else:
                                   ds = xarray.open_dataset( file_path, decode_times=False, decode_coords=False )

                               try:  ds.close()
                               except Exception: pass

                               good_files = good_files + 1
                               if self._verbose:
                                   print( f" OK: {file_path}" )

                           except Exception as err:
                               print(f"{err}")
                               errors.append( f"{file_path}, {err}\n" )

            print( f"Walked file system from {base_dir}, found {good_files} good files, {len(errors)} bad files.")

        return errors




