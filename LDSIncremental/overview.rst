LDSINCR
=======

This script replicates data in the LINZ Data Service to a local data store. Currently supported datastores include
PostgreSQL (PostGIS), MSSQL Spatial, FileGDB and SpatiaLite. Support for other outputs is ongoing. 
Running this script requires a destination (somewhere to write output) and valid LDS connection parameters.   


USAGE
-----

The basic usage of the script is:

``python ldsreplicate.py <options> output``        

Where valid output specifiers include; pg (postgres), ms (mssql), slite (spatialite) and fgdb (filegdb)

| Where options are:
| ``-f (--fromdate)`` Date in yyyy-mm-dd format start of incremental range (Omission assumes auto incremental bounds)
| ``-t (--todate)`` Date in yyyy-mm-dd format for end of incremental range (Omission assumes auto incremental bounds)
| ``-l (--layer)`` Layer name/id in format v:x### (IMPORTANT. Omission assumes ALL layers)
| ``-s (--source)`` Connection string for source DS
| ``-d (--destination)`` Connection string for destination DS
| ``-c (--cql)`` Filter definition in CQL format
| ``-h (--help)`` Display this message

The fromdate and todate options when used together trigger a LDS request for the changeset between the two requested dates. Normally such a request would follow a full replication request and update an existing dataset.

The layer option is mandatory. The layer is specified as the layer id in 'v:x###' format. Layer names are not necessarily unique so cannot be used to specify layers. Alternatively the user may set this option to 'ALL' and request download of all available LDS layers. Note. Only layers that are configured in the respective layer.properties file will be downloaded.

The source and destination options allow the user to specify their own connection string on the command line rather than using the values assigned in the 'ldsincr.conf' file. The validity of this connection string is not checked and may cause the request to fail.

A CQL filter can also be applied to select by geographical bounds or layer features. Filters are provided as text the syntax of which is not explicitly checked and could potentially cause a failure.   



EXAMPLES
--------

1. Copy the 'NZ Primary Parcels' layer using the auto incremental feature to a postgres output. The postgres connection parameters will have had to be defined in the ldsincr.conf configuration file.

   ``python ldsreplicate.py -l v:x772 pg``

2. Copy the 'NZ Railway Centre Lines' layer between the dates 2012-05-01 and 2012-05-31 to a SpatiaLite data store. Connections parameters for SpatialLite should be defined in the configuration file.

   ``python ldsreplicate.py -l v:x781 -f 2012-05-01 -t 2012-05-31 slite``

3. Copy the 'NZ Non-Primary Parcels' for all dates but only the Otago region into an MSSQL Spatial database whose parameters are stored in the configuration file

   ``python ldsreplicate.py -l v:x782 -c "land_district = 'Otago'" mssql``

4. Copy the 'NZ Land Districts' layer into the FileGDB database file somefile.gdb using incremental range determined by lastmodified and current date. 

   ``python ldsreplicate.py -l v:x785 -d "C:\path\to\some\folder\somefile.gdb" fgdb``
   
5. Copy the ASpatial 'ASP Name Associations' layer to Postgres FileGDB database file somefile.gdb using incremental range determined by lastmodified and current date. 

   ``python ldsreplicate.py -l v:x1209 -d "PG:"dbname='mypostgisdb' host='mypostgisdb.oracle.com' port='5432' user='scott' password='tiger'"``

  

NOTES
-----

1. Connection options can be set up in the config file "ldsincr.conf" but command line options can be used instead and will override configuration file settings. There is no checking done on command line connection strings
2. A valid layer name or the keyword ALL is required on the command line prefixed by the -l option flag
3. Dates are not required and omitting them will trigger an auto incremental request based on the lastmodified parameter in the respective destination layer properties file
4. Dates when provided must be in the format yyyy-mm-dd or specify the keyword ALL to trigger full replication
5. Full replication uses a fast driver copy mechanism and ignores discards and filters. Adding discard columns to a fully replicated layer would be considered a schema change and potentially fail.
6. CQL strings are only minimally checked for tokens indicating a predicate. The user is responsible for ensuring they are constructing their filter using the correct parameter names, formats and bounds.
7. CQL filters will override one another (they do not stack) according to; Command Line > configfile (ldsincr.conf) > layer config (___.layer.properties) 
8. Output names (for DB tables/FileGDB directories etc) are defined in the layer config file, not LDS. Changing this name will trigger the creation of a new table.
9. Adding multiple output on the command line will trigger the copy process for these outputs 
10. The properties files are interchangeable and selected based on their name ie postgresql.layer.properties is the layer properties file for the PostgreSQL output. Copying this file to mssql.layer.properties would assign it as the same for a MSSQLSpatial output
11. In the properties files the sections are denoted by the layer id in square brackets eg [v:x111]
12. Beneath the section header property values include {pkey, name, lastmodified, geocolumn, epsg, cql}

   * 12.1. Property 'pkey'. Defines the LDS primary key for this layer. Nominally set to 'id' this may very per layer.
   * 12.2. Property 'name'. Used as the name for the output table/file. Nominally set to LDS layer name which are not guaranteed to be unique
   * 12.3. Property 'category'. User editable field to enable grouping of common layers. Nominally initialised with the WFS keyword field.
   * 12.4. Property 'lastmodified'. This is a date string (yyyy-mm-dd) indicating the age of the data copied to output. It is used as a start point when doing auto-incremental updates.
   * 12.5. Property 'geocolumn'. The name for the output geometry column
   * 12.6. Property 'index'. Value indicating the type of index to create on the output table; 'spatial' or 'primary'. Alternatively the user can specify columns individually
   * 12.7. Property 'epsg'. Specifies the required EPSG number to affect a projection change. If left blank the source projection will be retained
   * 12.8. Property 'discard'. User specified fields to be excluded from output.
   * 12.9. Property 'cql'. Sets a cql filter for the layer. 
      * 12.9.1. The user is responsible for constructing well-formed CQL filters (for their platform)
      * 12.9.2. Layer filters will be overriden globally in the config file or on the command line.
      
13. The SpatiaLite driver will not return ASpatial layers. This is problematic when attempting to update Aspatial layers since we cannot read previously stored layers. An easy workaround is to completely reload aspatial layers as needed.
14. FileGDB fails to create layers with non ESRI formatted Spatial References. When importing to FileGDB we employ the OGR MorphtoESRI function but success is not assured. SR title overwriting works but may result in spatial inconsistencies. Users should be aware of these potential issues
15. GDAL does not support 64 bit integers. The current workaround forces the use of the feature-by-feature copy mechanism where we can transform the integer fiekds to string. Presently these fields are identified when they contain the string key 'sufi' in their name and for named tables only. Tables are listed in the main config file under [Misc]/64bitlayers
16. Large layers are delivered incomplete over WFS. The main layer where we see this is NZ Primary Parcels, v:x772. For now we ignore such layers by listing them in the main config file under [Misc]/problemlayers  


- Write problems in the FileGDB driver are not addressed in GDAL 1.9.1 and full support is only available in nightly builds > ~July  