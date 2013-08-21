@echo off
SET PWD=%~dp0

REM Setup Local Environment

set PATH=%PWD%bin;%PWD%bin\gdal;%PWD%bin\gdal\apps;%PWD%bin\gdal\plugins;%PWD%bin\gdal-data
set PATH=%PATH%;%PWD%apps\python27;%PWD%apps\ldsreplicate
set GDAL_DATA=%PWD%bin\gdal-data
set GDAL_DRIVER_PATH=%PWD%bin\gdal\plugins
set PYTHONHOME=%PWD%apps\python27
set PYTHONPATH=%PYTHONHOME%;%PYTHONHOME%\DLLs;%PYTHONHOME%\Lib
set PYTHONPATH=%PYTHONPATH%;%PYTHONHOME%\lib\site-packages;%PYTHONHOME%\lib\site-packages\osgeo

REM Execute LDS Replicate

"%PWD%apps\python27\python.exe" "%PWD%apps\ldsreplicate\ldsreplicate.py" %*    