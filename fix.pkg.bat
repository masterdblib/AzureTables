@echo off
cls

set curr_dir=%cd%

chdir /D src\app


.paket\paket.exe install --redirects --clean-redirects
.paket\paket.exe add MasterDbLib.Lib.dll --project MasterDbLib.AzureTables --force --redirects --clean-redirects --create-new-binding-files
.paket\paket.exe simplify

chdir /D %curr_dir%