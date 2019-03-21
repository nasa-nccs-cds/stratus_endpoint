from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
from netCDF4 import Dataset
import os, glob, yaml

class FileScanner:

    def __init__(self, **kwargs ):
        self.paths = {}
        self.files = []
        self.nFiles = 0
        self.base = None
        self.scan( **kwargs )
        self.partition()

    def __str__(self):
        return f"FileScanner[{self.nFiles}]: \n\tbase = {self.base}\n\tpaths = {self.paths}\n\t"

    def scan( self, **kwargs ):
        glob1 =  kwargs.get( "glob", None )
        globs: List[str] = kwargs.get( "globs", [] )
        if glob1 is not None: globs.append(glob1)
        path = kwargs.get("path",None)
        if path is not None:
            ext = kwargs.get("ext", None)
            if ext[0] == ".": ext = ext[1:]
            glob2 = path + "/**" if ext is None else path + "/**/*." + ext
            globs.append(glob2)
        if len(globs) > 0:
            print( "Scanning globs:" + str(globs) )
            paths = glob.glob( *globs, recursive=True)
            self.base = os.path.commonprefix([ os.path.dirname(path) for path in paths ] )
            self.nFiles = len(paths)
            self.files = [path[len(self.base):] for path in paths ]
        else: raise Exception( "No files found")

    def partition( self ):
        for relPath in self.files:
            basePath = os.path.dirname(relPath)
            fileList = self.paths.setdefault(basePath,[])
            fileList.append( relPath[len(basePath):].strip("/") )

    def filePath(self, index: int = 0 ) -> str:
        return f"{self.base}/{self.files[index]}"

    def yaml(self):
        collection = {}
        dataset = Dataset( self.filePath() )
        dims = { name: dim.size for name, dim in dataset.dimensions.items() }
        vars = {name: [ dim.name for dim in var.get_dims() ] for name, var in dataset.variables.items()}
        collection["files"] = self.paths
        collection["dimensions"] = dims
        collection["variables"] = vars
        collection["parameters"] =  dict( basePath=self.base, nFiles=self.nFiles )
        return yaml.dump(collection)


if __name__ == "__main__":

    scanner2 = FileScanner( path="/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/DAILY", ext="nc" )
    print( scanner2.yaml() )
    print(scanner2.files)



