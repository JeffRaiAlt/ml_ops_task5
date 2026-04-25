@echo off
setlocal

rem project root =dvc.yaml
for %%i in ("%~dp0...") do set "PROJECT_ROOT=%%~fi"

rem .venv PROJECT_ROOT
for %%i in ("%PROJECT_ROOT%\..\..\..") do set "VENV_ROOT=%%~fi"

set "PYTHON=%VENV_ROOT%\.venv\Scripts\python.exe"
set "VENV_SCRIPTS=%VENV_ROOT%\.venv\Scripts"

echo PROJECT_ROOT=%PROJECT_ROOT%
echo VENV_ROOT=%VENV_ROOT%
echo PYTHON=%PYTHON%

if not exist "%PYTHON%" (
    echo ERROR: Python not found
    exit /b 1
)

rem
set "PATH=%VENV_SCRIPTS%;%PATH%"

cd /d "%PROJECT_ROOT%"


goto :skip_comment
"%PYTHON%" -m dvc stage add -f -n prepare ^
  -d src/prepare.py ^
  -d data/raw/ecommerce.csv ^
  -p prepare ^
  -o data/processed ^
  "%PYTHON%" src/prepare.py
rem :skip_comment

"%PYTHON%" -m dvc stage add -f -n train ^
  -d src/train.py ^
  -d data/processed ^
  -p train ^
  -o model.pkl ^
  "%PYTHON%" src/train.py
:skip_comment

"%PYTHON%" -m dvc %* 