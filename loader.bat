@echo off
setlocal EnableExtensions

cd /d "%~dp0"
set "PROJECT_DIR=%CD%\"
set "VENV_PYTHON=%PROJECT_DIR%.venv\Scripts\python.exe"

echo CSV to ClickHouse loader
echo Project: %PROJECT_DIR%
echo.

call :find_uv
if not defined UV_CMD call :install_uv
if not defined UV_CMD goto :uv_not_found

echo Installing the locked environment
echo Running: uv sync --locked --extra dev
"%UV_CMD%" sync --locked --extra dev
if errorlevel 1 goto :sync_failed

if not exist "%VENV_PYTHON%" goto :venv_missing

echo Checking runtime imports
"%VENV_PYTHON%" -c "import streamlit, pandas, clickhouse_connect; print('dependencies OK')"
if errorlevel 1 goto :import_failed

echo.
echo Starting the application
rem Порт, версию копии и запуск Streamlit решает csv_click.launcher: у модуля
rem есть тесты, которые его ИСПОЛНЯЮТ, а этот файл проверяется только чтением.
set "PYTHONPATH=%PROJECT_DIR%src"
echo Running: python -m csv_click.launcher
"%VENV_PYTHON%" -m csv_click.launcher
if errorlevel 1 goto :launch_failed
goto :eof

:find_uv
rem uv ищется на PATH, а затем в каталоге пользовательских скриптов Python.
rem Второе нужно потому, что на рабочей машине без прав администратора
rem единственный проходящий способ установки - pip install --user uv, а он
rem кладёт uv.exe туда, чего на PATH обычно нет.
set "UV_CMD="
for /f "delims=" %%U in ('where uv 2^>nul') do (
    set "UV_CMD=%%U"
    goto :eof
)
for %%L in ("py -3.12" "py" "python") do (
    for /f "delims=" %%D in ('%%~L -c "import os,sysconfig;d=sysconfig.get_path('scripts','nt_user');print(d if os.path.exists(os.path.join(d,'uv.exe')) else '')" 2^>nul') do (
        if not "%%D"=="" (
            set "UV_CMD=%%D\uv.exe"
            echo Found uv outside PATH: %%D\uv.exe
            goto :eof
        )
    )
)
goto :eof

:install_uv
rem Раньше здесь печатался совет, и на этом всё заканчивалось. Пользователь без
rem прав администратора - аналитик: команду он скопирует, но это лишний шаг, на
rem котором он останавливается и идёт спрашивать. Ставим сами тем Python, который
rem найдём; после установки uv.exe лежит ВНЕ PATH, поэтому ищем заново.
for %%L in ("py -3.12" "py" "python") do (
    if not defined UV_CMD (
        %%~L -c "import sys" >nul 2>nul
        if not errorlevel 1 (
            rem Факт запоминается сразу: без него итог винил отсутствие Python,
            rem хотя строкой выше сам сообщал, что нашёл рабочий.
            set "PYTHON_FOUND=%%~L"
            echo uv is missing. Installing it with %%~L
            %%~L -m pip install --user uv
            if errorlevel 1 (
                echo   pip could not install uv with %%~L
            ) else (
                call :find_uv
            )
        )
    )
)
goto :eof

:uv_not_found
echo.
if defined PYTHON_FOUND goto :uv_install_failed
echo ERROR: uv is missing and there is no working Python to install it with.
echo The environment is installed from uv.lock so that library versions cannot
echo drift between two loads. Install uv, reopen this folder, and run loader.bat again.
echo.
echo Without administrator rights:
echo   py -3.12 -m pip install --user uv
echo With administrator rights:
echo   winget install --source winget -e --id astral-sh.uv
pause
exit /b 1

:uv_install_failed
rem Python есть и работает - повторять ту же команду руками бессмысленно, она
rem только что не сработала. Причина почти всегда в доступе к индексу пакетов:
rem на этих машинах стоит корпоративный перехват TLS.
echo ERROR: Python works (%PYTHON_FOUND%), but uv could not be installed.
echo The message from pip above says why. The usual causes:
echo   1. No access to the package index, or a proxy in front of it.
echo   2. pip is missing in this interpreter.
echo   3. --user is refused because this interpreter is inside a virtual environment.
echo.
echo Ask for uv to be installed centrally, or with administrator rights run:
echo   winget install --source winget -e --id astral-sh.uv
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

:launch_failed
rem Не «упал Streamlit»: модуль может выйти с ошибкой ещё до его запуска -
rem например, когда все порты в его диапазоне заняты. Причину печатает он сам.
echo.
echo ERROR: the application stopped with an error. The reason is printed above.
pause
exit /b 1
