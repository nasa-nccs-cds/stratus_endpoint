from stratus_endpoint.collections.scan import FileScanner
import pprint
from glob import glob
pp = pprint.PrettyPrinter(depth=4).pprint

#scanner2 = FileScanner( "merra-daily-test", path="/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/DAILY", ext="nc" )

#scanner2.write( "/tmp/" )

test_path = "/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/DAILY"

pp( glob( "/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/DAILY/**/*.nc", recursive=True ) )