@echo off
REM Setup Local Environment

set PATH=%~dp0bin\gdal;%~dp0apps\python27;%~dp0apps\ldsreplicate;%~dp0bin\gdal\bin;%~dp0bin\gdal\gdalplugins;%~dp0bin\gdal\gdal-data
set GDAL_DATA=%~dp0bin\gdal\gdal-data
set GDAL_DRIVER_PATH=%~dp0bin\gdal\gdalplugins
set PYTHONHOME=%~dp0apps\python27
set PYTHONPATH=%~dp0apps\python27;%~dp0apps\python27\DLLs;%~dp0apps\python27\lib;%~dp0apps\python27\lib\lib-tk;%~dp0apps\python27\lib\site-packages;%~dp0apps\python27\lib\site-packages\osgeo
set PROJ_LIB=%~dp0bin\GDAL\projlib

REM Execute LDS Replicate

python apps\ldsreplicate\ldsreplicate.py %*    