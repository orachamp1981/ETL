@echo off
setlocal EnableDelayedExpansion

REM ===============================
REM Python Configuration
REM ===============================
set "PYTHON_HOME=C:\Users\AST\AppData\Local\Programs\Python\Python311"
set "PYTHON_EXE=%PYTHON_HOME%\python.exe"

REM ===============================
REM Base Directories
REM ===============================
set "BASE_DIR=H:\Workspace\GENERAL_PYTHON"
set "LOG_DIR=%BASE_DIR%\logs"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

REM ===============================
REM Date & Time Safe Formatting
REM ===============================
for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set DT=%%I

set YYYY=!DT:~0,4!
set MM=!DT:~4,2!
set DD=!DT:~6,2!
set HH=!DT:~8,2!
set MI=!DT:~10,2!
set SS=!DT:~12,2!

set "LOG_FILE=%LOG_DIR%\etl_insurance_!YYYY!!MM!!DD!_!HH!!MI!!SS!.log"

REM ===============================
REM Execute ETL
REM ===============================
"%PYTHON_EXE%" "%BASE_DIR%\etl_insurance_loader.py" > "%LOG_FILE%" 2>&1

echo ETL finished. Log created at:
echo %LOG_FILE%
