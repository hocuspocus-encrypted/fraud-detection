@echo off
REM run_dashboard.bat
REM Launches the Streamlit dashboard using the venv's Python directly via
REM `-m streamlit`, avoiding the same launcher-script path issues we hit
REM with pip.exe and spark-submit.cmd.

set PROJECT_DIR=%~dp0

"%PROJECT_DIR%venv\Scripts\python.exe" -m streamlit run "%PROJECT_DIR%streamlit_dashboard.py"

pause
