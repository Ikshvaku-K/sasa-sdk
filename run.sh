#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
pip install -q fastapi "uvicorn[standard]" aiofiles python-multipart
echo ""
echo "Starting Spark job in background..."
python spark/streaming_job.py &
echo "Starting SASA server → http://localhost:8001"
echo "  Dashboard : http://localhost:8001/dashboard"
echo "  Demo page : http://localhost:8001/demo"
echo "  SDK       : http://localhost:8001/sdk/sasa.js"
echo "  API docs  : http://localhost:8001/docs"
echo ""
cd backend && uvicorn main:app --host 0.0.0.0 --port 8001
