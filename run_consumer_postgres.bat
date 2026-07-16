@echo off
REM run_consumer_postgres.bat
REM Launches the Spark Structured Streaming consumer that writes raw
REM transactions to Postgres (spark_consumer_postgres.py), with every
REM environment variable hardcoded to sidestep Windows PATH/alias issues.
REM
REM Adjust the paths below ONCE if your Java/Hadoop install locations differ.

set PROJECT_DIR=%~dp0
set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot
set PATH=%JAVA_HOME%\bin;%PATH%
set HADOOP_HOME=C:\hadoop
set PATH=%HADOOP_HOME%\bin;%PATH%
set SPARK_LOCAL_HOSTNAME=127.0.0.1
set SPARK_LOCAL_IP=127.0.0.1
set PYSPARK_PYTHON=%PROJECT_DIR%venv\Scripts\python.exe
set PYSPARK_DRIVER_PYTHON=%PROJECT_DIR%venv\Scripts\python.exe

echo Using Java:   %JAVA_HOME%
echo Using Hadoop: %HADOOP_HOME%
echo Using Python: %PYSPARK_PYTHON%
echo.

"%PROJECT_DIR%venv\Scripts\spark-submit.cmd" --conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.postgresql:postgresql:42.7.4 "%PROJECT_DIR%spark_consumer_postgres.py"

pause
