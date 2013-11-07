# LDS Replicate - README

The LDS Replication script replicates data in the LINZ Data Service to a local data store. Currently supported datastores include; PostgreSQL (PostGIS), MSSQL Spatial, FileGDB and SpatiaLite. Support for other outputs is ongoing.


# QUICKSTART

Run the installer, "lds-replicate-setup-win32-r#.#.#.exe"
Click through presented options and fill in the dialogs as they appear

## SETUP WIZARD
This first dialog sets up LDS and database connections where the user can enter their API key and database connection strings. For multiple connection instances this dialog can be run independently or the config file can be edited manually.

### Setup Wizard. Front Page
1. Enter the name for your user config file. eg. "my_file" will become __install-dir__/apps/ldsreplicate/conf/my-file.conf 
2. Enter your LINZ API key. eg. __1234567890abcdef1234567890abcdef__ Clicking the link titled LDS API Key on this page will take you to the LDS Key page 
3. Select the type of data output you want to generate. eg. PostgreSQL, MSSQLSpatial, FileGDB, SQLite
4. Select internal[x] or external[_] eg internal create a file in the conf directory listing layer details. Selecting external creates this same list in table form inside your selected data source
5. Select encryption if you want your password encrypted before saving. (NB Care with this [trial] option as passwords may become corrupted between releases)
  
### Proxy Page
1. Enter Proxy server. (IP or URL) eg. 127.0.0.1 or localhost
2. Enter Proxy port. eg. 3128
3. Select Proxy authorisation method, Basic, NTLM (Windows NTLM proxy auth), Digest, Any
4. Enter Username and Password if needed
	
### PostgreSQL Page
1. Enter Server. (IP or URL) eg. 127.0.0.1 or localhost
1. Enter Port. eg. 5432
2. Enter DBName
3. Enter Schema. eg. public
4. Enter User/Pass
	
### MSSQLSpatial Page
1. Enter Server\Instance (one string)
2. Enter DBName
3. Enter Schema. eg. dbo
4. Select Trusted/Untrusted connection (Use Windows user/pass combination)
5. If Untrusted enter User/Pass
	
### FileGDB Page
Enter filegdb directory location. eg. <path>/dir-name.gdb/
NB. Directory name must end with a .gdb suffix. The setup wizard will attempt to initialise a new database for you if the named database doesn't exist

### SQLite Page
Enter spatialite file location. eg. <path>/file-name.sqlite
NB. File name must end with a .db, .sqlite or .sqlite3 suffix. The setup wizard will attempt to initialise a new database for you if the named database doesn't exist
	
### Confirmation Page
If the connection was successful the final page will be displayed. Confirm the details are correct and click okay.	If this confitmation page fails to appear you may need to edit the connection details you provided in the database page.
	
## LAYER CONFIG

In this window you can create, delete and edit groups of layers. 

* The top left data entry box filters the results in the layer window below. 
* The available-layers window, bottom left, shows all available layers that haven't currently been assigned to an active group. 
* Groups are designated by selecting or entering a name in the top right box and pushing selected layers from the available layers window to the group layers window. 
* The grouped-layers window, bottom right, displays toe contents of the active group. Layers can be added and deleted from the group using the left/right selection arrows.

Groups are designated by adding a keyword (the group name) to the keywords field of the layer config file/table.

Clicking __Reset__ re-reads the layer information from LDS resetting all previously created groups and modification dates. Clicking finish closes the group selection dialog.
   
## MAIN GUI

The Main GUI is the primary window where we interact with LDS and replicate layers. It displays a number of fields defining the replication to be performed.

##### Destination. 
This dropdown displays only output types that have been configured by the user. It indicates the output type to generate, indicating to the program which drivers to load and initialising any connection parameters. Running the Main Config Wizard or editing the user config file will expand the selection presented here.

##### Group/Layer.
This selects the Layer or the predefined Group to replicate. To open and (re)define groups click the __Layer Select__ button in the lower left corner.

##### User Config
This dropdown allows you to choose the user config file you created in the initial setup. NOTE. Because a user config can contain the connection parameters for a number of connection instances the content of the file must contain a component that corresponds to the Destination selection made above.

##### EPSG
Enabling this option with the checkbox allows you to select the output spatial reference from the accompanying dropdown. If not enabled the spatial reference of the source is used by default. If you have selected this option in a previous replication the EPSG value is saved to the layer config file and used in all subsequent replications. This applies per layer and does not depend on group memebership. When replicating a group, enabling a destination will force the selected spatial reference across all members of the group overwriting any previously saved assignments. Care needs to be taken here that spatial references are consistent with previous replications. 

##### From Date
Enabling this option allows the user to set an incremental start date. No data from before this date will be returned by the subsequent LDS resuest.

##### To Date
This option sets the incremental end date. No data from after this date will be returned in subsequent LDS requests. 

###### NOTE
Only layers with a valid primary key can will be queried incrementally. Setting dates from layers without a primary key will generate non-incremental queries.




	
## MANUAL CONFIG
1. Using configuration files
2. Create a copy of the master template.conf file and rename it. 
3. Edit this file adding in your own parameters to override the defaults set in the master copy.
4. In the [LDS] section add your own API key.
5. Select either internal/external for the config parameter to choose between an in-db or file based layer-config table.
6. Select parameters for your desired output e.g. [PostgreSQL] host/port/dbname etc 
7. From the command line run the command "ldsreplicate -u _your-custom-conf-file_ -l _layer-to-replicate_ [_initialise-layer-config-file_] _output-format_"

        python LDSReader/ldsreplicate.py -u myprefs.conf -l v:x785 init pg
      

8. Using command line parameters
9. From the command line run the command "ldsreplicate -x|-i -s _LDS-URL_ -d _destination-connection-string_ -l _layer-to-replicate_ [_initialise-layer-config-file_] _output-format_"

        python ldsreplicate.py -x -s http://wfs.data.linz.govt.nz/<your-api-key>/v/x785/wfs?service=WFS&request=GetFeature&typeName=v:x785 -d PG:"dbname='<your-db-name>' host='<your-host-name>' port='<your-port-number>' user='<your-user-name>' password='<your-password>'" -l v:x785 init pg

Note. 
The 'init' command initialises the layer config file, reading settings from the WFS GetCapabilities document. This should be used once, when first running ldsreplicate
The 'clean' command cleans the selected layer, deleting the table and resetting its last-modified entry in the layer config file. 



# COMMAND LINE USAGE

The basic usage of the script is:

python ldsreplicate.py _options_ output        

Where valid output specifiers include; pg (postgres,postgresql), ms (mssql,mssqlserver), sl (slite,spatialite) and fg (fgdb,filegdb)

where options are:

* -f (--fromdate) Date in yyyy-mm-dd format start of incremental range (omission assumes auto incremental bounds)
* -t (--todate) Date in yyyy-mm-dd format for end of incremental range (omission assumes auto incremental bounds)
* -l (--layer) Mandatory. Layer name/id in format v:x###
* -s (--source) Connection string for source DS
* -d (--destination) Connection string for destination DS
* -c (--cql) Filter definition in CQL format
* -x (--external) Override config, make layer conf external
* -i (--internal) Override config, make layer conf internal
* -h (--help) Display this message

where arguments are:
init. Initialise the layer-conf file rebuilding from get capabilities (Sets all lastmodified to None)
clean. Clean the selected-layer|all-layers deleting all data and resetting its layer-conf last modified entry (Careful with this!)


# EXAMPLES

    python ldsreplicate.py -l v:x772 pg

Copy the 'NZ Primary Parcels' layer using the auto incremental feature to a postgres output. The postgres connection 
parameters will have had to be defined in the template.conf configuration file.

    python ldsreplicate.py -u myconf -l v:x781 -f 2012-05-01 -t 2012-05-31 slite

Copy the 'NZ Railway Centre Lines' layer between the dates 2012-05-01 and 2012-05-31 to a SpatiaLite data store. Connections parameters for SpatialLite 
should be defined in the user configuration file, myconf.conf.

    python ldsreplicate.py -u myconf -l v:x782 -c "land_district = 'Otago'" mssql

Copy the 'NZ Non-Primary Parcels' layer for all dates, but only the Otago region, into an MSSQL Spatial database whose parameters are stored in the myconf.conf configuration file

    python ldsreplicate.py -l v:x785 -c "C:\path\to\some\folder\somefile.gdb" fgdb

Copy the 'NZ Land Districts' layer into the fileGDB database file somefile.gdb using incremental range determined by lastmodified and current date. 
    
    

# NOTES

1. Connection options should be set in the user config file overriding defaults in the main config file _template.conf_ . Alternatively, command line parameters can be used instead which will override all config file settings. There is only minimal checking done on command line connection strings

2. A valid layer name is required on the command line to select the desired layer. This is prefixed by the -l option flag. Leaving this option 
blank selects all layers. (NB. you can avoid this requirements by using a custom LDS URL, which necessarily must contain a layer component) 

3. Dates are not required, using them however, triggers an incremental request using the provided dates. Incremental requests will also occur if a lastmodified property has been saved in the layer config file/table.

4. Dates when provided must be in the format yyyy-mm-dd

5. Incremental requests are generally slower. Because locally saved tables are updated, rather than flushed and rewritten, remediation between primary key fields is necessary between LDS and these saved tables. For each update or delete uses an expensive _attribute filter_ operation is performed. For smaller updates (on large tables) this is more efficient. For large updates (on small tables) it may be faster to flush and rebuild. NOTE. Because a primary key field is needed to match tables, only those that contain a primary key are able to be updated incrementally. Layers labelled in __bold__ in the Layer Selection Dialog have this primary key.

6. CQL strings are only minimally checked for tokens that might indicate a valid predicate. The user is responsible for ensuring they are constructing their filter using the correct parameter names, formats and bounds.

7. CQL filters will override one another (they do not stack) according to; Command Line > configfile (template.conf) > layer config (___.layer.properties/lds_config) 

8. Output names (for DB tables/FileGDB directories etc) are defined in the layer config file, not LDS. Changing this name will trigger the creation of a new table.

9. Adding multiple outputs on the command line will trigger the copy process for these outputs. 

10. The properties files are interchangeable and selected based on their name i.e. postgresql.layer.properties is the layer properties file for the PostgreSQL output. Copying this file to mssql.layer.properties would assign to an MSSQLSpatial output keeping all cql, discarded columns and modification dates.

11. In the properties files the sections are denoted by the layer id in square brackets eg. [v:x111]. In the properties table the ID column takes this role.

12. Beneath the section header property values include {pkey, name, lastmodified, geocolumn, epsg, cql}

13. Property 'pkey'. Defines the LDS primary key for this layer. Nominally set to 'id' this may vary per layer. NOTE. The included file, _ldspk.csv_ contains the reference primary keys used by the layer config initialisation script.

14. Property 'name'. Used as the name for the output table/file these are not guaranteed to be unique. Nominally set to LDS layer name.

15. Property 'lastmodified'. This is a date string (yyyy-mm-dd) indicating the age of the data copied to output. It is used as a start point when doing incremental updates.

16. Property 'geocolumn'. Used as the name for the output geometry column nominally set to _shape_.

17. Property 'epsg'. Specifies the required EPSG number to affect a projection change. If left blank the source projection will be retained. 
NB. If a new EPSG has been specified the desired projection is stored in the layer config file/table for future reference (This assignment will survive subsequent cleaning). Replicating a layer/group will not rewrite any previous EPSG assignments unless these are explictly requested. If a layer is incrementally capable changing the EPSG will have the affect of creating any new features in the requested SR but leaving existing features in the original. These rules apply to group EPSG assignment

18. Property 'cql'. Sets a cql filter for the layer. 
  1. The user is responsible for constructing well-formed CQL filters. 
  2. Layer filters will be overriden globally in the config file or on the command line.

   
19. Write problems in the FileGDB driver are not addressed in GDAL 1.9.1 and full support is only available in nightly builds from July 2012 

20. Additional fixes are implemented in GDAL 1.9.2 including SR handling (for ESRI) and attribute filters. This is the minimum version tested. Earlier versions may cause problems
