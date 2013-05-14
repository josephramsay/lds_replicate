@echo off
:BEGIN

SET PWD=%~dp0

REM Setup Local Environment

set PATH=%PWD%bin\gdal;%PWD%apps\python27;%PWD%apps\ldsreplicate;%PWD%bin\gdal\bin;%PWD%bin\gdal\gdalplugins;%PWD%bin\gdal\gdal-data
set GDAL_DATA=%PWD%bin\gdal\gdal-data
set GDAL_DRIVER_PATH=%PWD%bin\gdal\gdalplugins
set PYTHONHOME=%PWD%apps\python27
set PYTHONPATH=%PWD%apps\python27;%PWD%apps\python27\DLLs;%PWD%apps\python27\lib;%PWD%apps\python27\lib\lib-tk;%PWD%apps\python27\lib\site-packages;%PWD%apps\python27\lib\site-packages\osgeo
set PROJ_LIB=%PWD%bin\GDAL\projlib

set PSTR=import sys,os;sys.path.append(os.path.join('%PWD:~0,-1%','apps','ldsreplicate'));

REM Execute LDS Replicate GUI

IF '%1'=='W' GOTO WIZZ
IF '%1'=='L' GOTO LWIZZ

REM ----------------------------------------------------------------------------
:GUI
echo Starting LDS Replicate GUI

"%PWD%apps\python27\python.exe" -c "%PSTR%from ldsreplicate_gui import main; main()"

GOTO END

REM ----------------------------------------------------------------------------
:WIZZ
echo Starting Configuration Wizard
"%PWD%apps\python27\python.exe" -c "%PSTR%from ldsreplicate_gui import conf; conf()"

GOTO END

REM ----------------------------------------------------------------------------
:LWIZZ
echo Starting Layer Configuration Selector
REM cd %PWD%\apps\ldsreplicate
"%PWD%apps\python27\python.exe" -c "%PSTR%from ldsreplicate_gui import lconf; lconf()"

GOTO END

REM ----------------------------------------------------------------------------
:END