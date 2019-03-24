from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
from netCDF4 import Dataset, num2date, Variable
from functools import total_ordering
import dateutil.parser
import os, glob, yaml, cftime, datetime, argparse

@total_ordering
class FileRec:
    def __init__(self, path ):
        self.path = path
        dataset = Dataset(path)
        self.units = "minutes since 1970-01-01T00:00:00Z"
        self.relPath = None
        self.base_date = datetime.datetime(1970,1,1,1,1,1)
        vars_list = list(dataset.variables.keys())
        vars_list.sort()
        self.varsKey = ",".join(vars_list)
        time_var = dataset.variables["time"]
        time_data = time_var[:]
        if len(time_data) > 1:
            dt = time_data[1] - time_data[0]
            self.start_date: cftime.real_datetime = num2date(time_data[0], time_var.units, dataset.calendar)
            self.end_date: cftime.real_datetime = num2date(time_data[-1] + dt, time_var.units, dataset.calendar)
        else:
            self.start_date: cftime.real_datetime = num2date(time_data[0], time_var.units, dataset.calendar)
            self.end_date: cftime.real_datetime = self.start_date
        self.start_time_value = self.getTimeValue( self.start_date )
        self.end_time_value = self.getTimeValue( self.end_date )
        self.size = len(time_data)

    def getTimeValue(self, date: cftime.real_datetime) -> int:
        offset = date - self.base_date
        return offset.days * 24 * 60 + int(round(offset.seconds/60))

    def setBase(self, base: str ):
        self.relPath = self.path[len(base):]

    def __eq__(self, other: "FileRec") -> bool:
        return self.start_time_value == other.start_time_value

    def __ne__(self, other: "FileRec") -> bool:
        return not (self.start_time_value == other.start_time_value)

    def __lt__(self, other: "FileRec"):
        return self.start_time_value < other.start_time_value

    def __str__(self):
        return  f"fREC -[{self.start_date}]-  -[{self.end_date}]-  {self.path} "

    @staticmethod
    def time( frec: "FileRec" ):
        return frec.start_time_value


class  FileScanner:

    def __init__(self, collectionId: str, **kwargs ):
        self.aggs = {}
        self.varPaths = {}
        self.collectionId = collectionId
        print( f"Running FileScanner with args: {kwargs}")
        self.scan( **kwargs )

    def __str__(self):
        aggs = [ f"---> {varId}:\n{agg}" for varId,agg in self.aggs.items() ]
        return "\n".join( aggs )

    def scan( self, **kwargs ):
        glob1 =  kwargs.get( "glob" )
        globsArg =  kwargs.get( "globs" )
        globs: List[str] = [] if globsArg is None else globsArg.split(",")
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
            for path in paths:
                frec = FileRec( path )
                self.varPaths.setdefault( frec.varsKey, [] ).append( frec )
            for varKey, frecList in self.varPaths.items():
                frecList.sort()
                base = os.path.commonprefix( [ os.path.dirname(frec.path) for frec in frecList ] )
                size = 0
                for frec in frecList:
                    size += frec.size
                    frec.setBase(base)
                agg = Aggregation( base, frecList, size )
                self.aggs[ varKey ] = agg
        else: raise Exception( "No files found")

    def write( self, collectionsDir: str ):
        baseDir = os.path.expanduser(collectionsDir)
        os.makedirs( baseDir, exist_ok=True )
        collectionsFile = f"{baseDir}/{self.collectionId}.csv"
        lines = []
        with open( collectionsFile, 'w' ) as f:
            for aggId,agg in self.aggs.items():
                aggFile = f"{baseDir}/{agg.getId(self.collectionId)}.ag1"
                agg.write( aggFile )
                lines.append(f"# title, {self.collectionId}\n")
                lines.append(f"# dir, {baseDir}\n")
                lines.append(f"# format, ag1\n")
                for var in agg.vars:
                    lines.append( f"{var}, {aggFile}\n")
            f.writelines(lines)

# From EDAS: writeAggregation
class Aggregation:

    def __init__(self, base: str, frecList: List[FileRec], nTs: int ):

        self.nFiles = len(frecList)
        self.fileRecs = frecList
        self.base = base
        self.nTs = nTs
        self.vars = set()
        self.paths = self.partition()
        self.lines = self.process()

    def varKey(self):
        return "--".join(self.vars)

    def getId(self, collectionId: str ):
        return collectionId + "--" + self.varKey()

    def __str__(self):
        return f"FileScanner[{self.nFiles}]: \n\tbase = {self.base}\n\tpaths = {self.paths}\n\t"

    def partition( self ):
        paths = {}
        for frec in self.fileRecs:
            basePath = os.path.dirname(frec.relPath)
            fileList = paths.setdefault(basePath,[])
            fileList.append( frec.relPath[len(basePath):].strip("/") )
        return paths

    def filePath(self, index: int = 0 ) -> str:
        return f"{self.base}/{self.fileRecs[index].relPath}"

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

    def write(self, filePath: str ):
        with open( filePath, 'w' ) as f:
            f.writelines( self.lines )

    def process(self):
        dataset = Dataset(self.filePath())
        nc_dims = [dim for dim in dataset.dimensions]
        nc_vars = [var for var in dataset.variables]
        lines = []
        lines.append(f'P; base.path; {self.base}\n')
        lines.append(f'P; num.files; {self.nFiles}\n')
        lines.append(f'P; time.nrows; {self.nTs}\n')
        lines.append(f'P; time.start; {self.fileRecs[0].start_time_value}\n')
        lines.append(f'P; time.end; {self.fileRecs[-1].end_time_value}\n')
        lines.append(f'P; time.calendar; {dataset.calendar}\n')
        for attr_name in dataset.ncattrs():
            lines.append(f'P; {attr_name}; {dataset.getncattr(attr_name)}\n')
        for vname in nc_vars:
            if vname not in nc_dims:
                self.vars.add(vname)
                var: Variable = dataset.variables[vname]
                dims = var.dimensions
                shape = [ str(self.nTs) if dims[iDim] == "time" else str(var.shape[iDim]) for iDim in range(len(dims)) ]
                lines.append(f'V; {vname}; {var.long_name}; {var.long_name}; {var.comments}; {",".join(shape)}; {1.0}; {" ".join(dims)}; {var.units}\n')
        for vname in nc_vars:
            if vname in nc_dims:
                coord: Variable = dataset.variables[vname]
                lvname = vname.lower()
                if lvname == "time":
                    lines.append(f'C; {vname} {self.nTs}\n')
                    lines.append(f'A; {vname}; {vname}; T; {str(self.nTs)}; minutes since 1970-01-01T00:00:00Z; {self.fileRecs[0].start_time_value}; {self.fileRecs[-1].end_time_value}\n')
                else:
                    lines.append(f'C; {vname} {coord.shape[0]}\n')
                    cdata = coord[:]
                    ctype = "?"
                    shape = [ str(s) for s in coord.shape ]
                    if   lvname.startswith("lat"): ctype = "Y"
                    elif lvname.startswith("lon"): ctype = "X"
                    elif lvname.startswith("lev") or lvname.startswith("plev"): ctype = "Z"
                    lines.append(f'A; {vname}; {vname}; {ctype}; {",".join(shape)}; {coord.units}; {cdata[0]}; {cdata[-1]}\n')
        for frec in self.fileRecs:
            lines.append(f'F; {frec.start_time_value}; {frec.size}; {frec.relPath}\n')
        return lines


if __name__ == "__main__":
    parser = argparse.ArgumentParser( description='Scan the file system to create a collection', prog="cscan", epilog="Must set the environment variable 'HPDA_COLLECTIONS_DIR'\n" )
    parser.add_argument('collectionName', help='A name for the collection')
    parser.add_argument('-path', help='The top level directory containing all files for the collection')
    parser.add_argument('-ext', help="The file extension for all files in the collection (used only with '-path', default: nc)", default="nc")
    parser.add_argument('-globs', help='A comma-separated list of unix file system globs for selecting files in the collection')
    parser.add_argument('-glob', help='A single unix file system glob for selecting files in the collection')
    args = parser.parse_args()
    scanner = FileScanner( args.collectionName, **vars(args) )
    collectionsDir = os.environ.get('HPDA_COLLECTIONS_DIR')
    assert collectionsDir is not None, "Must set the HPDA_COLLECTIONS_DIR environment variable"
    scanner.write( collectionsDir )

#    scanner2 = FileScanner( path="/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/DAILY", ext="nc" )

