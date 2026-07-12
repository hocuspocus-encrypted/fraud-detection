@echo off
REM run_producer.bat
REM Launches the Kafka producer using the venv's Python directly, bypassing
REM any PATH ambiguity between this venv and a global Anaconda install.

set PROJECT_DIR=%~dp0

"%PROJECT_DIR%venv\Scripts\python.exe" "%PROJECT_DIR%kafka_producer.py" --interval 0.5

pause
