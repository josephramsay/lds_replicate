@echo off
:BEGIN
SET PWD=<inst-dir>
                                                                                
                                                                                

set PATH=%PWD%bin;%PWD%bin\gdal;%PWD%bin\gdal\apps;%PWD%bin\gdal\plugins;%PWD%bin\gdal-data
set PATH=%PATH%;%PWD%apps\python27;%PWD%apps\ldsreplicate
set GDAL_DATA=%PWD%bin\gdal-data
set GDAL_DRIVER_PATH=%PWD%bin\gdal\plugins
set PYTHONHOME=%PWD%apps\python27
set PYTHONPATH=%PYTHONHOME%;%PYTHONHOME%\DLLs;%PYTHONHOME%\Lib
set PYTHONPATH=%PYTHONPATH%;%PYTHONHOME%\lib\site-packages;%PYTHONHOME%\lib\site-packages\osgeo

set PSTR=import sys,os;sys.path.append(os.path.join('%PWD:~0,-1%','apps','ldsreplicate'));

REM Execute LDS Replicate GUI

IF '%1'=='G' GOTO GUI
IF '%1'=='W' GOTO WIZZ
IF '%1'=='L' GOTO LWIZZ

echo Select LDS GUI [G], Setup Wizard [W] or Layer Select dialog [L] 
SET /P ldsw=">"

IF %ldsw%==G GOTO GUI
IF %ldsw%==W GOTO WIZZ
IF %ldsw%==L GOTO LWIZZ

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
"%PWD%apps\python27\python.exe" -c "%PSTR%from ldsreplicate_gui import lconf; lconf()"

GOTO END

REM ----------------------------------------------------------------------------
:END