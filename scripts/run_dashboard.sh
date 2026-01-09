#!/bin/bash
# Script to run Streamlit dashboard

echo "🚀 Starting ETL Pipeline Dashboard..."
echo ""
echo "Dashboard will be available at: http://localhost:8501"
echo "Press Ctrl+C to stop"
echo ""

streamlit run dashboard.py --server.port 8501 --server.address localhost

