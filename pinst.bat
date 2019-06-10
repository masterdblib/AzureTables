@echo off
cls

set curr_dir=%cd%

chdir /D src\app

.paket\paket.exe install --redirects --clean-redirects

if errorlevel 1 (
  exit /b %errorlevel%
)

.paket\paket.exe install --redirects --clean-redirects

chdir /D %curr_dir%