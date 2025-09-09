@echo off
REM Job Data Synchronizer - Windows Batch Runner
REM ============================================

echo Job Data Synchronizer - Windows Runner
echo ========================================

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo Error: Python is not installed or not in PATH
    echo Please install Python 3.6+ and try again
    pause
    exit /b 1
)

REM Check if virtual environment exists
if exist "venv\Scripts\activate.bat" (
    echo Activating virtual environment...
    call venv\Scripts\activate.bat
) else (
    echo Warning: Virtual environment not found
    echo Using system Python installation
)

REM Install dependencies if needed
echo Checking dependencies...
pip install -r requirements.txt >nul 2>&1

REM Check if configuration exists
if not exist "job_sync_config.json" (
    if not exist ".env" (
        echo.
        echo Warning: No configuration file found!
        echo Please create job_sync_config.json or .env file
        echo See .env.job_sync for example configuration
        echo.
    )
)

REM Run the job synchronizer
echo.
echo Starting Job Data Synchronizer...
echo.
python run_job_sync.py

echo.
echo Press any key to exit...
pause >nul
