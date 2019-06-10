@echo off
cls

set curr_dir=%cd%

chdir /D src\app

.paket\paket.exe install --redirects --clean-redirects

if errorlevel 1 (
  exit /b %errorlevel%
)
 
.paket\paket.exe install --redirects --clean-redirects

dotnet build
chdir /D MasterDbLib.AzureTables.Lib
dotnet restore
dotnet build

chdir /D %curr_dir%

.paket\paket.exe install --redirects --clean-redirects

if errorlevel 1 (
  exit /b %errorlevel%
)

.paket\paket.exe install --redirects --clean-redirects

build.bat
