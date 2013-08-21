set PATH=%~dp0bin;%~dp0bin\gdal;%~dp0bin\gdal\apps;%~dp0bin\gdal\plugins;%~dp0bin\gdal-data
set PATH=%PATH%;%~dp0apps\python27;%~dp0apps\ldsreplicate
set GDAL_DATA=%~dp0bin\gdal-data
set GDAL_DRIVER_PATH=%~dp0bin\gdal\plugins
set PYTHONHOME=%~dp0apps\python27
set PYTHONPATH=%~dp0apps\python27;%~dp0apps\python27\DLLs;%~dp0apps\python27\Lib
set PYTHONPATH=%PYTHONPATH%;%~dp0apps\python27\lib\site-packages;%~dp0apps\python27\lib\site-packages\osgeo