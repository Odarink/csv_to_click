@echo off
setlocal EnableExtensions

cd /d "%~dp0"
set "PROJECT_DIR=%CD%\"
set "VENV_PYTHON=%PROJECT_DIR%.venv\Scripts\python.exe"
set "APP_PATH=src\csv_click\app.py"

echo CSV to ClickHouse loader
echo Project: %PROJECT_DIR%
echo.

call :find_python
if not defined PYTHON_CMD goto :python_not_found

if not exist "%VENV_PYTHON%" (
    echo Creating virtual environment in .venv
    echo Running: python -m venv .venv
    %PYTHON_CMD% -m venv .venv
    if errorlevel 1 goto :venv_failed
)

if not exist "%VENV_PYTHON%" goto :venv_missing

echo Updating pip
echo Running: python -m pip install --upgrade pip
"%VENV_PYTHON%" -m ensurepip --upgrade >nul 2>&1
"%VENV_PYTHON%" -m pip install --upgrade pip
if errorlevel 1 goto :pip_failed

echo Installing dependencies from requirements.txt
echo Running: python -m pip install -r requirements.txt
"%VENV_PYTHON%" -m pip install -r requirements.txt
if errorlevel 1 goto :deps_failed

echo Checking runtime imports
"%VENV_PYTHON%" -c "import streamlit, pandas, clickhouse_connect; print('dependencies OK')"
if errorlevel 1 goto :import_failed

echo.
echo Starting Streamlit on http://localhost:8501
set "PYTHONPATH=%PROJECT_DIR%src"
echo Running: python -m streamlit run src\csv_click\app.py --server.address 127.0.0.1 --server.port 8501
"%VENV_PYTHON%" -m streamlit run "%APP_PATH%" --server.address 127.0.0.1 --server.port 8501
if errorlevel 1 goto :streamlit_failed
goto :eof

:find_python
set "PYTHON_CMD="

py -3.12 -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)" >nul 2>&1
if not errorlevel 1 (
    set "PYTHON_CMD=py -3.12"
    goto :eof
)

py -3.11 -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)" >nul 2>&1
if not errorlevel 1 (
    set "PYTHON_CMD=py -3.11"
    goto :eof
)

python -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)" >nul 2>&1
if not errorlevel 1 (
    set "PYTHON_CMD=python"
    goto :eof
)

goto :eof

:python_not_found
echo.
echo ERROR: Python 3.11 or newer was not found.
echo Install Python 3.12, reopen this folder, and run loader.bat again.
echo.
echo Suggested install command:
echo winget install -e --id Python.Python.3.12
pause
exit /b 1

:venv_failed
echo.
echo ERROR: Could not create .venv.
pause
exit /b 1

:venv_missing
echo.
echo ERROR: .venv\Scripts\python.exe was not created.
pause
exit /b 1

:pip_failed
echo.
echo ERROR: pip update failed.
pause
exit /b 1

:deps_failed
echo.
echo ERROR: dependency installation from requirements.txt failed.
echo Check internet access or package index availability, then run loader.bat again.
pause
exit /b 1

:import_failed
echo.
echo ERROR: required Python packages are not importable.
pause
exit /b 1

:streamlit_failed
echo.
echo ERROR: Streamlit stopped with an error.
echo If port 8501 is busy, close the process that uses it and run loader.bat again.
pause
exit /b 1
