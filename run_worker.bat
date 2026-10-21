@echo off
REM run_worker.bat
REM Launches the investigation worker (PLAN.md Phase 3) with the venv's Python.
REM Settings, the provider (MODEL_PROVIDER) and the kill switch (WORKER_ENABLED) come from .env.

set PROJECT_DIR=%~dp0

cd /d "%PROJECT_DIR%"
"%PROJECT_DIR%venv\Scripts\python.exe" "%PROJECT_DIR%investigation_worker.py"

pause
