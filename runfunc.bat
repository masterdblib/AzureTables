@echo off
cls

set curr_dir=%cd%

chdir /D src\app

.paket\paket.exe restore
if errorlevel 1 (
  exit /b %errorlevel%
)

.paket\paket.exe restore

chdir /D MasterDbLib.AzureTables.Function

rmdir /s /q bin\Debug\net462\publish 2>nul

dotnet restore
dotnet build
dotnet publish

chdir /D bin\Debug\net472\publish

func host start

chdir /D %curr_dir%
