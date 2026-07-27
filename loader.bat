@echo off
setlocal EnableExtensions

cd /d "%~dp0"
set "PROJECT_DIR=%CD%\"
set "VENV_PYTHON=%PROJECT_DIR%.venv\Scripts\python.exe"
set "APP_PATH=src\csv_click\app.py"

echo CSV to ClickHouse loader
echo Project: %PROJECT_DIR%
echo.

where uv >nul 2>&1
if errorlevel 1 goto :uv_not_found

echo Installing the locked environment
echo Running: uv sync --locked --extra dev
uv sync --locked --extra dev
if errorlevel 1 goto :sync_failed

if not exist "%VENV_PYTHON%" goto :venv_missing

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

:uv_not_found
echo.
echo ERROR: uv was not found.
echo The environment is installed from uv.lock so that library versions cannot
echo drift between two loads. Install uv, reopen this folder, and run loader.bat again.
echo.
echo Suggested install command:
echo winget install -e --id astral-sh.uv
pause
exit /b 1

:sync_failed
echo.
echo ERROR: uv sync --locked --extra dev failed. Two likely causes:
echo   1. uv.lock no longer matches pyproject.toml. --locked refuses to launch on a
echo      stale lock on purpose, because a silently re-resolved environment makes
echo      before/after load timings incomparable. Run: uv lock
echo   2. No internet access or the package index is unreachable.
pause
exit /b 1

:venv_missing
echo.
echo ERROR: .venv\Scripts\python.exe was not created by uv sync.
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
