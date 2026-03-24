#!/bin/sh
set -e

# Simple static+API server using Flask only for API; UI served by simple HTTP
# but to keep this simple, we serve UI via Flask static in production.

export PYTHONUNBUFFERED=1

# Start Flask API
python -m flask run --host=$FLASK_HOST --port=$FLASK_PORT
