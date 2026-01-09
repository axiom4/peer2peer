#!/bin/bash
echo "Stopping all python nodes..."
pkill -f "src/main.py"
pkill -f "src/web_ui.py"
echo "Stopped."
