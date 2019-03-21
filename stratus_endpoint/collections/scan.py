from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
import os, glob

class FileScanner:

    def __init__(self, **kwargs ):
        self.paths = {}
        self.base, files = self.scan( **kwargs )
        self.nFiles = len( files )
        self.partition(files)

    def __str__(self):
        return f"FileScanner[{self.nFiles}]: \n\tbase = {self.base}\n\tpaths = {self.paths}\n\t"

    @classmethod
    def scan( cls, **kwargs ) -> Tuple[str,List[str]]:
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
            base = os.path.commonprefix(paths)
            relPaths = [path[len(base):] for path in paths ]
            return base, relPaths
        else: raise Exception( "No files found")

    def partition( self, files: List[str] ):
        for relPath in files:
            basePath = os.path.dirname(relPath)
            fileList = self.paths.setdefault(basePath,[])
            fileList.append( relPath[len(basePath):].strip("/") )


if __name__ == "__main__":

    scanner2 = FileScanner( path="/usr/local/web/stratus", ext="py" )
    print( scanner2 )



