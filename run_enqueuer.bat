@echo off
REM run_enqueuer.bat
REM Launches the investigation enqueuer (PLAN.md D2) with the venv's Python.
REM Settings come from .env.

set PROJECT_DIR=%~dp0

cd /d "%PROJECT_DIR%"
"%PROJECT_DIR%venv\Scripts\python.exe" "%PROJECT_DIR%investigation_enqueuer.py"

pause
