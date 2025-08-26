#!/bin/bash

# Move to parent directory (project root)
cd "$(dirname "$0")/.."

# Activate virtual environment
source .venv/bin/activate

# Install/update dependencies
echo "📦 Installing/updating dependencies..."
pip install -r requirements.txt

# Launch Streamlit dashboard
echo "🚀 Launching AI Papers Dashboard..."
streamlit run dashboard.py

# Deactivate virtual environment when done
deactivate
