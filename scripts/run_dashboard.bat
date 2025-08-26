@echo off

REM Move to parent directory (project root)
cd /d "%~dp0\.."

REM Activate virtual environment
call .venv\Scripts\activate.bat

REM Install/update dependencies
echo 📦 Installing/updating dependencies...
pip install -r requirements.txt

REM Launch Streamlit dashboard
echo 🚀 Launching AI Papers Dashboard...
streamlit run dashboard.py

REM Deactivate virtual environment when done
call .venv\Scripts\deactivate.bat

pause
