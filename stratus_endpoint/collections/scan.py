from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
from netCDF4 import Dataset, num2date
import os, glob, yaml

class FileRec:
    def __init__(self, path ):
        self.path = path
        dataset = Dataset(path)
        vars_list = list(dataset.variables.keys())
        vars_list.sort()
        self.varsKey = ",".join(vars_list)
        dataset.get_variables_by_attributes()


class  FileScanner:

    def __init__(self, **kwargs ):
        self.aggs = {}
        self.varPaths = {}
        self.scan( **kwargs )

    def __str__(self):
        aggs = [ f"---> {varId}:\n{agg}" for varId,agg in self.aggs.items() ]
        return "\n".join( aggs )

    def varKey(self, path: str ) -> str:
        pass

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
            for path in paths:
                dataset = Dataset( path )
                time_var = dataset.variables["time"]
                time_data = time_var[:]
                if len( time_data ) > 1:
                    dt = time_data[1] - time_data[0]
                    start_date = num2date(time_data[0],time_var.units,dataset.calendar)
                    end_date =  num2date(time_data[-1] + dt, time_var.units, dataset.calendar )
                else:
                    start_date = num2date(time_data[0], time_var.units, dataset.calendar)
                    end_date = start_date
                print( f"Dataset:  -[{start_date}]-  -[{end_date}]-  {path} " )
                vars_list = list(dataset.variables.keys())
                vars_list.sort()
                varsKey = ",".join( vars_list )
                self.varPaths.setdefault( varsKey, [] ).append( path )
            for varKey, varPathList in self.varPaths.items():
                base = os.path.commonprefix([ os.path.dirname(path) for path in varPathList ] )
                files = [path[len(base):] for path in varPathList ]
                agg = Aggregation( base, len(varPathList), files )
                self.aggs[ varKey ] = agg
        else: raise Exception( "No files found")

class Aggregation:

    def __init__(self, base: str, nFiles: int, files: List[str] ):
        self.paths = {}
        self.files = files
        self.nFiles = nFiles
        self.base = base
        self.partition()

    def __str__(self):
        return f"FileScanner[{self.nFiles}]: \n\tbase = {self.base}\n\tpaths = {self.paths}\n\t"

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

    def write(self, filePath: str ):
        with open( filePath, 'w' ) as f:
            f.write(f'P; base.path; {self.base}')
            f.write(f'P; num.files; {self.nFiles}')

# bw.write( s"P; time.nrows; ${nTimeSteps}\n")
# bw.write( s"P; time.start; ${startTime}\n")
# bw.write( s"P; time.end; ${endTime}\n")
# bw.write( s"P; time.calendar; ${calendar.name}\n")

if __name__ == "__main__":
    scanner2 = FileScanner( path="/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/DAILY", ext="nc" )
    print( scanner2 )




# def writeAggregation(aggFile: File, fileHeaders: IndexedSeq[FileHeader], format: String, maxCores: Int = 8): Unit = {
#     DiskCacheFileMgr.validatePathFile(aggFile)
#
#
# logger.info(s
# "Writing Aggregation[$format] File: " + aggFile.toString)
# val
# nReadProcessors = Math.min(Runtime.getRuntime.availableProcessors, maxCores)
# logger.info("Processing %d files with %d workers".format(fileHeaders.length, nReadProcessors))
# val
# bw = new
# BufferedWriter(new
# FileWriter(aggFile))
# val
# startTime = fileHeaders.head.startValue
# val
# calendar = fileHeaders.head.calendar
# val
# endTime = fileHeaders.last.endValue
# val
# nTimeSteps: Int = fileHeaders.foldLeft(0)(_ + _.nElem)
# val
# resolution = fileHeaders.head.resolution.map(item= > s
# "${item._1}:${item._2.toString}").mkString(",")
# val
# fileMetadata = FileMetadata(fileHeaders.head.toPath.toString, nTimeSteps)
# logger.info(" ")
# try {


# for (attr < - fileMetadata.attributes ) {bw.write( s"P; ${attr.getFullName}; ${attr.getStringValue} \n")}
# for (coordAxis < - fileMetadata.coordinateAxes; ctype = coordAxis.getAxisType.getCFAxisName ) {
# if (ctype.equals("Z") )      {bw.write( s"A; ${coordAxis.getShortName}; ${coordAxis.getDODSName}; $ctype; ${coordAxis.getShape.mkString(", ")}; ${coordAxis.getUnitsString};  ${coordAxis.getMinValue}; ${coordAxis.getMaxValue}\n")}
# else if (ctype.equals("T") ) {bw.write( s"A; ${coordAxis.getShortName}; ${coordAxis.getDODSName}; $ctype; ${nTimeSteps}; ${coordAxis.getUnitsString};  ${startTime}; ${endTime}\n")}
# else {bw.write( s"A; ${coordAxis.getShortName}; ${coordAxis.getDODSName}; $ctype; ${coordAxis.getShape.mkString(", ")}; ${coordAxis.getUnitsString};  ${coordAxis.getMinValue}; ${coordAxis.getMaxValue}\n" )}
# }
# for (cVar < - fileMetadata.coordVars) {
# if ( cVar.getShortName.toLowerCase.startsWith("tim") ) {
# bw.write(s"C; ${cVar.getShortName};  ${nTimeSteps} \n")
# } else {
# bw.write(s"C; ${cVar.getShortName};  ${cVar.getShape.mkString(", ")} \n")
# }
# }
# for (variable < - fileMetadata.variables) {bw.write( s"V; ${variable.getShortName}; ${variable.getFullName}; ${variable.getDODSName};  ${variable.getDescription};  ${getShapeStr(variable.getDimensionsString,nTimeSteps,variable.getShape)}; ${resolution}; ${variable.getDimensionsString};  ${variable.getUnitsString} \n" )}
# for (fileHeader < - fileHeaders) {
# bw.write( s"F; ${EDTime.toString(fileHeader.startValue)}; ${fileHeader.nElem.toString}; ${fileHeader.relFile}\n" )
# }
# } finally {
# fileMetadata.close
# }
# bw.close()
# }




