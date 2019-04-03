from stratus_endpoint.collections.scan import FileScanner

scanner2 = FileScanner( "merra-daily-test", path="/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/DAILY", ext="nc" )

scanner2.write( "/tmp/" )

