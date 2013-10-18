REM IExpress method (abs paths)
REM c:\WINDOWS\system32\iexpress.exe /N F:\git\LDS\LDSReplicate\LDS32.SED

REM BAT2EXE method (executes in tmp, breaks %~dp0)
"C:\Program Files\Bat2ExeConverter\Windows (32 bit)\Bat_To_Exe_Converter.exe" -bat F:\git\LDS\LDSReplicate\LDS32.bat -save "F:\git\LDS\LDSReplicate\LDS32.exe" -icon F:\icon\linz.ico -overwrite -invisible