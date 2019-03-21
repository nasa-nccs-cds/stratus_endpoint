from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
import os, glob

class FileList:

    def __init__(self, base: str, relPaths: List[str] ):
        self.base = base
        self.paths = relPaths

    def __str__(self):
        return f"FileList[ base = {self.base}, files = {self.paths} ]"

class CDScan:

    def __init__(self, **kwargs):
        self.name = ""

    def collectFiles(self, **kwargs ) -> FileList:
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
            return FileList( base, relPaths )
        else: raise Exception( "No files found")


if __name__ == "__main__":

    scanner = CDScan()

    fileList1 = scanner.collectFiles( glob="/usr/local/web/stratus/**/*.py")
    print( fileList1 )

    fileList2 = scanner.collectFiles( path="/usr/local/web/stratus", ext="py" )
    print( fileList2 )



