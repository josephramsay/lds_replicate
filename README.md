This script replicates data in the LINZ Data Service to a local data store. Currently supported datastores include
PostgreSQL (PostGIS), MSSQL Spatial, FileGDB and SpatiaLite. Support for other outputs is ongoing.


QUICKSTART
==========
Run the installer, "lds-replicate-setup-win32-r#.#.#.exe"
Click through presented options 

SETUP WIZARD
------------

Setup Wizard. Front Page
	1. Enter the name for your user config file. eg. "my_file" will become <install-dir>/apps/ldsreplicate/conf/my-file.conf 
	2. Enter your LINZ API key. eg. 1234567890abcdef1234567890abcdef/ Clicking the link titled LDS API Key on this page will take you to the LDS Key page 
	3. Select the type of data output you want to generate. eg. PostgreSQL
	4. Select internal[x] or external[_] eg internal create a file in the conf directory listing layer details. Selecting external creates this same list in table form inside your selected data source
	5. Select encryption if you want your password encrypted before saving. (NB Care with this option as passwords may be corrupted between releases)
	
Setup Wizard. Proxy Page
	1. Enter Proxy server. (IP or URL) eg. 127.0.0.1
	2. Enter Proxy port. eg. 5432
	3. Select Proxy authorisation method, Basic, NTLM (Windows NTLM proxy auth), Digest, Any
	4. Enter Username and Password if needed
	
Setup Wizard. PostgreSQL Page
	1. Enter Server/Port
	2. Enter DBName/Schema
	3. Enter User/Pass
	
Setup Wizard. MSSQLSpatial Page
	1. Enter Server\Instance (one string)
	2. Enter DBName/Schema
	3. Select Trusted/Untrusted connection (Use Windows uesr/pass combination)
	4. If Untrusted enter User/Pass
	
Setup Wizard. FileGDB Page
	1. Enter filegdb directory location. eg. <path>/dir-name.gdb/
	NB. Directory name must end with a .gdb suffix. The setup wizard will attempt to initialise a new database for you if the named database doesn't exist

Setup Wizard. SQLite Page
	1. Enter spatialite file location. eg. <path>/file-name.sqlite
	NB. File name must end with a .db, .sqlite or .sqlite3 suffix. The setup wizard will attempt to initialise a new database for you if the named database doesn't exist
	
Setup Wizard. Final Page
	1. If the connection was successful the final page will be displayed. Confirm the details are correct and click okay.	
	
LAYER CONFIG
------------

In this window you can create, delete and edit groups of layers. The top left data entry box filters the results in the layer window below. The layer window shows all available layers that haven't currently been assigned to an active group. Groups are designated by selecting or entering a name in the top right box and pushing selected layers from the available layers window to the group layers window. clicking reset re-reads the layer information from LDS resetting all previously created groups and modification dates. Clicking finish closes the group selection dialog.
   
MAIN GUI
--------




	
MANUAL CONFIG
-------------
1.    Using configuration files
1.1   Create a copy of the master template.conf file and rename it. 
1.2.  Edit this file adding in your own parameters to override the defaults set in the master copy.
1.3.  In the [LDS] section add your own API key.
1.4.  Select either internal/external for the config parameter to choose between an in-db or file based layer-config table.
1.5.  Select parameters for your desired output e.g. [PostgreSQL] host/port/dbname etc 
1.6.  From the command line run the command "ldsreplicate -u <your-custom-conf-file> -l <layer-to-replicate> [<initialise-layer-config-file>] <output-format>"
      e.g. python LDSReader/ldsreplicate.py -u myprefs.conf -l v:x785 init pg
      

2.    Using command line parameters
2.1   From the command line run the command "ldsreplicate -x|-i -s <LDS-URL> -d <destination-connection-string> -l <layer-to-replicate> [<initialise-layer-config-file>] <output-format>"
      e.g. python LDSReader/ldsreplicate.py -x -s http://wfs.data.linz.govt.nz/<your-api-key>/v/x785/wfs?service=WFS&request=GetFeature&typeName=v:x785 -d PG:"dbname='<your-db-name>' host='<your-host-name>' port='<your-port-number>' user='<your-user-name>' password='<your-password>'" -l v:x785 init pg

Note. 
The 'init' command initialises the layer config file, reading settings from the WFS GetCapabilities document. This should be used once, when first running ldsreplicate
The 'clean' command cleans the selected layer, deleting the table and resetting its last-modified entry in the layer config file. 



USAGE
=====

The basic usage of the script is:

python ldsreplicate.py <options> output        

Where valid output specifiers include; pg (postgres,postgresql), ms (mssql,mssqlserver), sl (slite,spatialite) and fg (fgdb,filegdb)

where options are:
-f (--fromdate) Date in yyyy-mm-dd format start of incremental range (omission assumes auto incremental bounds)
-t (--todate) Date in yyyy-mm-dd format for end of incremental range (omission assumes auto incremental bounds)
-l (--layer) Mandatory. Layer name/id in format v:x###
-s (--source) Connection string for source DS
-d (--destination) Connection string for destination DS
-c (--cql) Filter definition in CQL format
-x (--external) Override config, make layer conf external
-i (--internal) Override config, make layer conf internal
-h (--help) Display this message

where arguments are:
init. Initialise the layer-conf file rebuilding from get capabilities (Sets all lastmodified to None)
clean. Clean the selected-layer|all-layers deleting all data and resetting its layer-conf last modified entry (Careful with this!)


EXAMPLES
========

----1----
python ldsreplicate.py -l v:x772 pg

Copy the 'NZ Primary Parcels' layer using the auto incremental feature to a postgres output. The postgres connection 
parameters will have had to be defined in the ldsincr.conf configuration file.

----2----
python ldsreplicate.py -l v:x781 -f 2012-05-01 -t 2012-05-31 slite

Copy the 'NZ Railway Centre Lines' layer between the dates 2012-05-01 and 2012-05-31 to a SpatiaLite data store. Connections parameters for SpatialLite 
should be defined in the configuration file.

----3----
python ldsreplicate.py -l v:x782 -f ALL -c "land_district = 'Otago'" mssql

Copy the 'NZ Non-Primary Parcels' for all dates but only the Otago region into an MSSQL Spatial database whose parameters are stored in the configuration file

----4----
python ldsreplicate.py -l v:x785 -c "C:\path\to\some\folder\somefile.gdb" fgdb

Copy the 'NZ Land Districts' layer into the fileGDB database file somefile.gdb using incremental range determined by lastmodified and current date. 
    
    
---------

NOTES
=====
1. Connection options can be set up in the config file "ldsincr.conf" but command line options can be used instead and will override 
configuration file settings. There is only minimal checking done on command line connection strings
2. A valid layer name is required on the command line to select the desired layer. This is prefixed by the -l option flag. Leaving this option 
blank selects all layers. (NB. you can avoid this requirements by using a custom LDS URL, which necessarily must contain a layer component) 
3. Dates are not required. Omitting one date will trigger an auto incremental request based on the lastmodified parameter in the respective 
destination layer-properties file. Omitting both dates assumes a full update (non incremental) is needed
4. Dates when provided must be in the format yyyy-mm-dd
5. Full replication uses a fast driver copy mechanism and ignores discards and filters. Adding discard columns to a fully replicated layer
would be considered a schema change and potentially fail.
6. CQL strings are only minimally checked for tokens indicating a predicate. The user is responsible for ensuring they are constructing their
filter using the correct parameter names, formats and bounds.
7. CQL filters will override one another (they do not stack) according to; Command Line > configfile (ldsincr.conf) > layer config (___.layer.properties) 
8. Output names (for DB tables/FileGDB directories etc) are defined in the layer config file, not LDS. Changing this name will trigger the creation
of a new table.
9. Adding multiple output on the command line will trigger the copy process for these outputs 

10. The properties files are interchangeable and selected based on their name i.e. postgresql.layer.properties is the layer properties file for the 
PostgreSQL output. Copying this file to mssql.layer.properties would assign to an MSSQLSpatial output keeping all cql, discarded columns and modification dates.
11. In the properties files the sections are denoted by the layer id in square brackets eg [v:x111]. In the properties table the id column takes this role.
12. Beneath the section header property values include {pkey, name, lastmodified, geocolumn, epsg, cql}
12.1. Property 'pkey'. Defines the LDS primary key for this layer. Nominally set to 'id' this may very per layer. Because Topo/Hydro layers dont use a primary key
this values is not set for these layers.
12.2. Property 'name'. Used as the name for the output table/file these are not guaranteed to be unique. Nominally set to LDS layer name.
12.3. Property 'lastmodified'. This is a date string (yyyy-mm-dd) indicating the age of the data copied to output. It is used as a start point 
when doing auto-incremental updates.
12.4. Property 'geocolumn'. Used as the name for the output geometry column
12.5. Property 'epsg'. Specifies the required EPSG number to affect a projection change. If left blank the source projection will be retained
12.6. Property 'cql'. Sets a cql filter for the layer. Notes 1: the user is responsible for constructing well-formed CQL filters. 2: Layer filters 
will be overriden globally in the config file or on the command line.

   

A1. Write problems in the FileGDB driver are not addressed in GDAL 1.9.1 and full support is only available in nightly builds > ~July 2012 
A2. Additional fixes implemented in GDAL 1.9.2 including SR handling (for ESRI) and attribute filters
